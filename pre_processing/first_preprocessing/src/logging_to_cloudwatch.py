import logging
import cloudwatch_handler

def log(path, stream_name):
    # CloudWatchHandler 생성
    handler = cloudwatch_handler.cloudwatch_handler()
    # 로깅 설정
    # logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    handler.set_init(path, stream_name)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("[%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

        
    return logger


    

# def main():
#     """
#     CloudWatch 로깅을 설정하고 다양한 로그 레벨의 메시지를 기록하는 메인 함수입니다.

#     수행 작업:
#     1. '/aws/lambda/crawler-programer' 경로에 'programers_logs'라는 이름으로 CloudWatch 로그 설정
#     2. 디버그, 정보, 경고, 오류, 심각 수준의 로그 메시지 기록

#     로그는 '/aws/lambda/{본인 크롤러 파일명}' 경로에 '{크롤러 이름}_logs'라는 이름 지정시 폴더생성 후 저장됩니다.
#     """
    
#     # example
#     logger = log('/aws/lambda/crawler-programer','programers_logs')
#     logger.debug("This in debug message")
#     logger.info("This in info message")
#     logger.warning("This in warning message")
#     logger.error("This in info message")
#     logger.critical("This in critical message")
    

# if __name__ == '__main__':
#     main()