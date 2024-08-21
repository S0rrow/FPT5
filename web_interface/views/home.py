import streamlit as st

def display_home_page(logger):
    '''
    show initial page before login
    '''
    logger.log(f"rendering home page...", name=__name__)
    try:
        st.header("Home")
        st.write("""
            # FPT5

            ## 목표
            이 프로젝트는 수많은 채용 사이트들에서 여러 공고들을 수집하고 분석해 특정 \
            업무 분야에 대해서 다양한 업무 분야에서 실제로 각광받거나 많이 사용되는 \
            기술 스택의 목록을 추천하는 시스템을 구성하기 위한 것입니다.
        """)
    except Exception as e:
        logger.log(f"Exception occurred while rendering home page: {e}", name=__name__)