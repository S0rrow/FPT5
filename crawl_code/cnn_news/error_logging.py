import os
import logging
import pathlib
from time import localtime, strftime


# def log(msg, flag=None):
#     if flag is None:
#         flag = 0

#     head = ["debug", "error", "status"]
#     now = strftime("%H:%M:%S", localtime())
#     log_directory = f'{BASEDIR}/logs'
#     if not os.path.exists(log_directory):
#         os.makedirs(log_directory)
#     logpath = os.path.join(log_directory, "./app.log")

#     try:
#         with open(logpath, "a") as logfile:
#             logfile.write(f"[{now}][{head[flag]}] > {msg}\n")
#     except Exception as e:
#         with open(logpath, "a") as logfile:
#             logfile.write(f"[{now}] > {e}\n")
#         print(f"exception on log function: {e}")


# Ensure the logs directory exists


def log():

    BASEDIR = os.getcwd()


    # Ensure the logs directory exists
    log_directory = f'{BASEDIR}/logs'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logging.basicConfig(filename=os.path.join(log_directory, 'crawl.log'), level=logging.DEBUG)

    try:
        # Example code
        logging.info("Script started")
        
        # Simulate a process
        for i in range(3):
            logging.debug(f"Processing item {i}")
        
        # Simulate an error
        raise ValueError("An example error occurred")

        # Simulate a successful completion
        logging.info("Script finished successfully")

    except Exception as e:
        logging.error("Error occurred", exc_info=True)

    print("Script executed")
        
# if __name__ == "__main__":
#     # BASEDIR = pathlib.Path(__file__).parent.resolve()
#     # log("This is an error message", flag=1)

#     log()
    