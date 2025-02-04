# Experiments with MERRA-21C cloud optimization

## Project organization

This project manages dependencies using pixi (https://pixi.sh/latest/).
Individual scripts can be run using `pixi run python <scriptname>`.
For interactive development work, you can use `pixi shell`.

Alternatively, you can create your own environment using conda/mamba or some other tool; refer to either the `pixi.lock` file (for exact versions of all packages) or the `pyproject.toml` (for less precise version information).

Note also that common functions used in some parts of the scripts are captured in a `m21c_experiments` package that you should install (in editable form).
These follow standard Python module organization --- the source lives in `src/m21c_experiments/`.

## Background on the data

The data are stored on NCCS Discover, in `/discover/nobackup/projects/gmao/merra21c/e5303_m21c_jan18/archive/SAMPLE_DAY`.
A lot of the code assumes a symlink from `./m21c_all` (in the project root) to `/discover/nobackup/projects/gmao/merra21c/e5303_m21c_jan18/archive/`; i.e., for reproducibility, in the project root, run:

```
ln -s /discover/nobackup/projects/gmao/merra21c/e5303_m21c_jan18/archive/ m21c_all
```

The files we are interested in are in `SAMPLE_DAY`.
This corresponds to MERRA-21C output for one complete day.

MERRA-21C has a large number of variables that are grouped into collections.
These collections can be divided into two major groups depending on the underlying spatial grid:

- `c360x360x6` files are raw model output distributed on a "cubed sphere". These outputs have an irregular shape in terms of lat/lon coordinates; therefore, each point has a unique lat/lon coordinate. The cube has 360 X coordinates, 360 Y coordinates, and 6 faces.
  - `slv` files here do not have a vertical dimension (e.g., column averages; surface diagnostics). Their dimensions are therefore: `{time: 1, face: 6, Y: 360, X: 360}`. The default chunking of these is one global chunk per face; i.e., `{1 x 1 x 360 x 360}`.
  - `v72` files have 72 vertical levels (e.g., vertical profiles), corresponding to the vertical slices of the cubed sphere. Their dimensions are: `{time: 1, face: 6, vertical: 72, Y: 360, X: 360}`. Their chunking is one global chunk per face and vertical level; i.e., `{1 x 1 x 1 x 360 x 360}`.
- `L1152x721` files are collections reprojected onto a regular lat-lon grid with 1152 longitudes and 721 latitudes.
  - `slv` files again do not have a vertical dimension. Their dimensions are `{time: 1, lat: 721, lon: 1152}`. Their default chunking is one global chunk (i.e., `{1 x 721 x 1152}`)
   -`p48` files have 48 vertical levels (corresponding to atmospheric pressure levels; hence, "p"). Their dimensions are `{time: 1, pressure: 48, lon: 721, lat: 1152}`. Their default chunking is one global chunk per vertical level, i.e., `{1 x 1 x 721 x 1152}`.
 
Each collection is identified by a tag like `aer_inst` or `odt_tavg`.
The first part tells you something about the kind of variables: e.g., `aer` is for aerosols; `flx` is for flux.
The second part tells you how to interpret the time coordinate:

- `inst` means instantaneous; i.e., the exact state at that exact moment in time (e.g., the mass of black carbon at exactly 12:00). The hourly results are distributed on the hour (e.g., 12:00, 1:00, 2:00...).
- `tavg` means time-averaged; e.g., the total amount of precipitation between 12:00 and 1:00, or the average temperature in that time window. The hourly results are distributed on the 30 minute mark that is the midpoint of the window (e.g., `time=12:30` means the average between 12:00 and 1:00)

## Objectives

We want to figure out how to structure the M21C data to (1) minimize the total data volume (i.e., optimizes compression and within-file space usage); (2) maximize read and analysis performance; and (3) minimize the impact on post-processing time (i.e., the post-processing script can't take too long).

Our degrees of freedom (in order of increasing user-facing impact) are:

1. Low-level file optimizations like paged aggregation
2. Internal chunking of the files (but keeping the overall organization the same)
3. Redo the overall organization to have, e.g., fewer variables per file but more time steps per file

#1 is basically done: I have already established pretty conclusively that **paged aggregation with an 8 MB chunk size** improves performance with negligible impacts on file size.
We still need to figure out #2 and #3.

### Chunking

Starting with #2 above.

Some initial results suggest that moving the cubed-sphere (`c360x360x6`) data from the default chunking (`time: 1, face: 1, vertical: 1, X: 360, Y: 360`) to smaller spatial (but more vertical) chunks --- e.g., `time: 1, face: 1, vertical: 36, X: 90, Y: 90` --- produces slightly smaller files and might have performance advantages for certain kinds of analysis (e.g., extracting a vertical profile).
However, applying the same general re-chunking to the lat-lon (`L1152x721`) data dramatically inflates the file size.
We need to explore a few different chunking structures and their impacts on file size and performance.
The default way to go about this is to develop a common set of benchmarks, then define a few different chunk structures and try them out one by one.
But, there may be cleverer ways.

### File organization

Time-permitting, we want to run some experiments on completely restructuring these files, such that, for a given simulation day, there is only one file per variable (and one variable per file); i.e., split up the files by variable, but aggregate all the time steps together.
Here, we need to measure (1) the overall change in data volume to make sure we haven't dramatically increased it; (2) the impact on performance and usability of these data; and (3) the time and CPU resources it takes to actually restructure the files (which may be nontrivial! We also want to figure out the most computationally efficient tool/workflow to do this, that can leverage parallelization, etc.).
