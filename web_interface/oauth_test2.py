import streamlit as st
from google.oauth2 import id_token, Credentials
from google.auth.transport import requests
import google_auth_oauthlib.flow
import json
import http.cookies

# OAuth 2.0 클라이언트 정보 (json 파일을 통해 불러오기)
with open("auth.json", "r") as f:
    client_secrets = json.load(f)["web"]

# OAuth 2.0 설정
SCOPES = ["https://www.googleapis.com/auth/userinfo.profile", 
          "https://www.googleapis.com/auth/userinfo.email", 
          "openid"]

# OAuth 2.0 Flow 객체 생성
flow = google_auth_oauthlib.flow.Flow.from_client_config(
    {
        "web": {
            "client_id": client_secrets["client_id"],
            "project_id": client_secrets["project_id"],
            "auth_uri": client_secrets["auth_uri"],
            "token_uri": client_secrets["token_uri"],
            "auth_provider_x509_cert_url": client_secrets["auth_provider_x509_cert_url"],
            "client_secret": client_secrets["client_secret"],
            "redirect_uris": client_secrets.get("redirect_uris", [])
        }
    },
    scopes=SCOPES
)

# 리디렉션 URI 설정
redirect_uri = 'http://localhost:8501'
flow.redirect_uri = redirect_uri

# Streamlit 앱 시작
st.title('Google OAuth 로그인 테스트')

# '로그인' 버튼
if 'credentials' not in st.session_state:
    if st.button('Google로 로그인'):
        authorization_url, state = flow.authorization_url(
            access_type='offline',
            include_granted_scopes='true')
        st.session_state['state'] = state
        st.markdown(f'[Google로 로그인하기]({authorization_url})')

# 인증 코드가 쿠키에서 전송되면 토큰을 가져옴
if 'state' in st.session_state:
    cookie_header = st.experimental_get_query_params().get('cookie')
    if cookie_header:
        try:
            # 쿠키에서 인증 코드 가져오기
            cookies = http.cookies.SimpleCookie(cookie_header[0])
            auth_code = cookies.get('auth_code').value

            # 인증 코드를 사용하여 토큰을 가져옴
            flow.fetch_token(code=auth_code)
            credentials = flow.credentials

            st.session_state['credentials'] = {
                'token': credentials.token,
                'refresh_token': credentials.refresh_token,
                'token_uri': credentials.token_uri,
                'client_id': credentials.client_id,
                'client_secret': credentials.client_secret,
                'scopes': credentials.scopes
            }

            st.success('로그인에 성공했습니다.')

        except Exception as e:
            st.error(f"인증 코드를 사용하여 토큰을 가져오는 중 오류 발생: {e}")
            st.session_state['credentials'] = None

# 로그인이 된 후 사용자 정보 가져오기
if 'credentials' in st.session_state:
    try:
        # 받은 자격 증명으로 사용자 정보 요청
        credentials_data = st.session_state['credentials']
        if credentials_data:
            credentials = Credentials(
                token=credentials_data.get('token'),
                refresh_token=credentials_data.get('refresh_token'),
                token_uri=credentials_data.get('token_uri'),
                client_id=credentials_data.get('client_id'),
                client_secret=credentials_data.get('client_secret'),
                scopes=credentials_data.get('scopes')
            )
            
            request = requests.Request()
            id_info = id_token.verify_oauth2_token(
                credentials.token, request, flow.client_config['client_id'])
            
            st.write(f"환영합니다, {id_info['name']}님!")
            st.image(id_info['picture'])
            st.write(f"이메일: {id_info['email']}")

        else:
            st.error("자격 증명 데이터가 올바르지 않습니다.")
        
    except Exception as e:
        st.error(f"받은 자격 증명으로 사용자 정보 요청 중 오류 발생: {e}")
        st.session_state['credentials'] = None
