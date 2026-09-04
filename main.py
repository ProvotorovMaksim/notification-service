from kafka_consumer import start_consuming
from asyncio import run
from logging import getLogger

logger = getLogger("main")
logger.setLevel("INFO")

def main():
    try:
        run(start_consuming())
    except KeyboardInterrupt:
        logger.info("Service stopped")
    except Exception as e:
        logger.error(f"There is an exception: {e}")
    finally:
        logger.info("Stopping")
        exit()

if __name__ == "__main__":
    main()