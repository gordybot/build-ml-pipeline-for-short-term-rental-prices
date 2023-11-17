#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info("Fetching artifact")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info("Reading dataframe")
    df = pd.read_csv(artifact_local_path)

    # Pre-processing
    logger.info("Starting pre-processing")
    
    # Drop outliers
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Convert last review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Export to csv.
    df.to_csv(args.output_artifact, index=False)

    # Log artifact to W&B
    logger.info("Logging artifact")
    artifact = wandb.Artifact(
            name = args.output_artifact,
            type = args.output_type,
            description= args.output_description
    )
    artifact.add_file(args.output_artifact)  
    run.log_artifact(artifact)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='Name:version of the raw data to clone from W&B',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Name for the cleaned data artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum nightly rental price to consider",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum nightly rental price to consider",
        required=True
    )


    args = parser.parse_args()

    go(args)
