import logging
logger = logging.getLogger("processing")
logger.setLevel(logging.INFO)


def tbc(event, context):
    logger.info("starting_ingestion_lambda")
    print("Great Success!")
    logger.info("Finished_ingestion_lambda")
