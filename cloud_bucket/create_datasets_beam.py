"""Creates a dataset - getting it ready to train a machine learning model and uploads into GCS."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta
import logging
import random
import uuid
import argparse

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
import ee
import os
import numpy as np
import requests

# Default values.
NUM_DATES = 15  # We could randomly sample 100 dates from a wider range, but here we specify 10 to reduce data size
MAX_REQUESTS = 10  # 20 is the default EE request quota
MIN_BATCH_SIZE = 20  # The min number of examples grouped together in each .npz file

# Constants.
NUM_BINS = 10  # categorise or "bin" continuous data values into discrete categories for stratified sampling
MAX_PRECIPITATION = 30  # found empirically
MAX_ELEVATION = 6000  # found empirically
PATCH_SIZE = 5  # fetching data from a 5x5 pixel area around each geographic point - effects granularity of data
END_DATE = datetime.now() - timedelta(days=30)  # setting to 30 days before current time

# Set the start date
START_DATE = datetime(2023, 1, 1)  # START_DATE = END_DATE - timedelta(days=30)  to get less data points

# Define a geographic area of interest as a list of (longitude, latitude) pairs
POLYGON = [(-140.0, 60.0), (-140.0, -60.0), (-10.0, -60.0), (-10.0, 60.0)]


def sample_points(date: datetime, num_bins: int = NUM_BINS) -> Iterator[tuple]:
    """
    Args:
        date: The date of interest.
        num_bins: Number of bins to bucketize values.

    Yields: (date, lon_lat) pairs.
    """
    from weather import data

    precipitation_bins = (
        data.get_gpm(date)
        .clamp(0, MAX_PRECIPITATION)
        .divide(MAX_PRECIPITATION)
        .multiply(num_bins - 1)
        .uint8()
    )
    elevation_bins = (
        data.get_elevation()
        .clamp(0, MAX_ELEVATION)
        .divide(MAX_ELEVATION)
        .multiply(num_bins - 1)
        .uint8()
    )
    unique_bins = elevation_bins.multiply(num_bins).add(precipitation_bins)
    points = unique_bins.stratifiedSample(
        numPoints=1,
        region=ee.Geometry.Polygon(POLYGON),
        scale=data.SCALE,
        geometries=True,
    )
    for point in points.toList(points.size()).getInfo():
        yield (date, point["geometry"]["coordinates"])


def get_training_example(
    date: datetime, point: tuple, patch_size: int = PATCH_SIZE
) -> tuple:
    """Gets an (inputs, labels) training example.

    Args:
        date: The date of interest.
        point: A (longitude, latitude) coordinate.
        patch_size: Size in pixels of the surrounding square patch.

    Returns: An (inputs, labels) pair of NumPy arrays.
    """
    from weather import data

    return (
        data.get_inputs_patch(date, point, patch_size),
        data.get_labels_patch(date, point, patch_size),
    )


def try_get_example(date: datetime, point: tuple) -> Iterator[tuple]:
    """Wrapper over `get_training_examples` that allows it to simply log errors instead of crashing."""
    try:
        yield get_training_example(date, point)
    except (requests.exceptions.HTTPError, ee.ee_exception.EEException) as e:
        logging.error(f" failed to get example: {date} {point}")
        logging.exception(e)


def write_npz(batch: list[tuple[np.ndarray, np.ndarray]], data_path: str) -> str:
    """Writes an (inputs, labels) batch into a compressed NumPy file.

    Args:
        batch: Batch of (inputs, labels) pairs of NumPy arrays.
        data_path: Directory path to save files to.

    Returns: The filename of the data file.
    """
    filename = FileSystems.join(data_path, f"{uuid.uuid4()}.npz")
    with FileSystems.create(filename) as f:
        inputs = [x for (x, _) in batch]
        labels = [y for (_, y) in batch]
        np.savez_compressed(f, inputs=inputs, labels=labels)
    logging.info(filename)
    return filename


def run(
    data_path: str,
    num_dates: int = NUM_DATES,
    num_bins: int = NUM_BINS,
    max_requests: int = MAX_REQUESTS,
    min_batch_size: int = MIN_BATCH_SIZE,
    beam_args: list[str] | None = None,
) -> None:
    """Runs an Apache Beam pipeline to create a dataset.

    This fetches data from Earth Engine and writes compressed NumPy files.

    Args:
        data_path: Directory path to save the data files.
        num_dates: Number of dates to extract data points from.
        num_bins: Number of bins to bucketize values.
        max_requests: Limit the number of concurrent requests to Earth Engine.
        min_batch_size: Minimum number of examples to write per data file.
        beam_args: Apache Beam command line arguments to parse as pipeline options.
    """
    random_dates = [
        START_DATE + (END_DATE - START_DATE) * random.random() for _ in range(num_dates)
    ]

    beam_options = PipelineOptions(
        beam_args,
        save_main_session=True,
        direct_num_workers=max(max_requests, MAX_REQUESTS),  # direct runner
        max_num_workers=max_requests,  # distributed runners
    )
    with beam.Pipeline(options=beam_options) as pipeline:
        (
            pipeline
            | "Random dates" >> beam.Create(random_dates)
            | "Sample points" >> beam.FlatMap(sample_points, num_bins)
            | "Reshuffle" >> beam.Reshuffle()
            | "Get example" >> beam.FlatMapTuple(try_get_example)
            | "Batch examples" >> beam.BatchElements(min_batch_size)
            | "Write NPZ files" >> beam.Map(write_npz, data_path)
        )


def main() -> None:
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    # Remove the --data-path argument as we'll get it from an environment variable
    parser.add_argument(
        "--num-dates",
        type=int,
        default=NUM_DATES,
        help="Number of dates to extract data points from.",
    )
    parser.add_argument(
        "--num-bins",
        type=int,
        default=NUM_BINS,
        help="Number of bins to bucketize values.",
    )
    parser.add_argument(
        "--max-requests",
        type=int,
        default=MAX_REQUESTS,
        help="Limit the number of concurrent requests to Earth Engine.",
    )
    parser.add_argument(
        "--min-batch-size",
        type=int,
        default=MIN_BATCH_SIZE,
        help="Minimum number of examples to write per data file.",
    )
    args, beam_args = parser.parse_known_args()

    # Construct the data_path using BUCKET_NAME from environment variable
    bucket_name = os.getenv('BUCKET_NAME')  # Returns None if BUCKET_NAME is not set
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable not set.")
    data_path = f"gs://{bucket_name}/weather/data"

    run(
        data_path=data_path,
        num_dates=args.num_dates,
        num_bins=args.num_bins,
        max_requests=args.max_requests,
        min_batch_size=args.min_batch_size,
        beam_args=beam_args,
    )

if __name__ == "__main__":
    main()

