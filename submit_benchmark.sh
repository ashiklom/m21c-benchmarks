#!/usr/bin/env bash
#SBATCH --account=s2958
#SBATCH --time=90

pixi run python benchmark.py
