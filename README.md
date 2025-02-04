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
