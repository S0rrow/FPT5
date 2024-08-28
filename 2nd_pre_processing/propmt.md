# wanted
[문맥]
당신은 IT-Software 전문 도메인을 가진 데이터 전문가입니다. 주어진 json형태의 데이터에서 필요한 데이터를 추출하거나 요약하는 임무를 맡았습니다.
[Instruction]
Input Data에서 주어진 json 포맷의 텍스트 데이터에서 `job_title`, `job_prefer`, `job_task`, `job_requirements` key의 value 값을 바탕으로 아래에 주어진 key 값들에 대한 value 값을 채워주세요. 단, 모를 경우 "null"을 넣어주세요.
- `tech_stack`: Input Data를 기반으로 유추하거나 추출한 개발에 사용되는 기술의 모음입니다. 여기에는 프로그래밍 언어, 프레임워크, 데이터베이스 시스템, 서버 인프라, 개발 도구 등 다양한 요소가 포함됩니다. 해당 key의 value는 항상 영단어로 이루어진 list여야 합니다.
- `job_requirements`: Input data를 기반으로 유추한 해당 직무의 요구사항입니다. value 값은 한글로 된 열 개 단어 이하인 문장 또는 단어이며, list 포맷으로 제시됩니다.
- `job_prefer`: Input data를 기반으로 유추한 해당 직무의 우대사항입니다. value 값은 한글로 된 열 개 단어 이하의 문장 또는 단어이며, list 포맷으로 제시됩니다.
- `job_category`: Input Data를 기반으로 유추하거나 추출한 직무(position) 분류 입니다. value 값은 영단어로 이루어진 list여야 합니다.
- `indurstry_type`: Input Data의 직무가 처리하는 산업 분류입니다. value 값은 영단어로 이루어진 list여야 합니다.
- `required_career`: Input Data에서 경력을 요구유무를 나타냅니다. 경력일 경우 `True` BOOLEAN값을, 신입일 경우 `False` BOOLEAN값을 넣어주세요.
[Input Data]
{'company_id': Decimal('18028'),
 'site_symbol': 'WAN',
 'job_title': '정보보안 담당자',
 'job_prefer': '정보보호 관련 학과 및 IT 관련학 전공 클라우드 보안솔루션 운영 경험이 있으신 분 인프라 취약점 점검 및 웹 모의해킹 수행 가능하신 분 ISO27701, ISMS-P 보안인증 대응 경험이 있으신 분 ISMS-P 인증심사원, CPPG, CISA, CISSP, 정보보안기사 자격증을 보유하신 분',
 'crawl_url': 'https://www.wanted.co.kr/wd/230608',
 'end_date': None,
 'job_id': Decimal('230608'),
 'crawl_domain': 'www.wanted.co.kr',
 'company_name': '차란차',
 'get_date': Decimal('1724605651'),
 'job_tasks': '정보보호 관련 규정,지침,정책을 수립, 관리 및 운영 정보보호 교육 계획을 수립하고 운영 개인정보위원회, 한국인터넷진흥원 등 외부 감사와 이슈 대응 ISMS-P 인증 준비 및 대응 활동 접근통제, 사내 보안 업무를 수행',
 'id': Decimal('2581867662'),
 'job_requirements': '5년 이상 10년 이하의 정보보호 업무 경험이 있으신 분 외부 감사 대응 경험이 있으신 분 ISMS-P 인증 경험이 있으신 분 다양한 플랫폼의 Public/Private Cloud 인프라 보안정책 설정 경험이 있으신 분 보안 솔루션 접근통제, 방화벽, 사용자 보안, VPN 경험이 있으신 분 개인정보처리시스템 현황 파악 및 관리 경험이 있으신 분 개인정보보호 정책, 지침 및 가이드에 대한 수립, 운영 및 관리 경험이 있으신 분'}

# jobkorea
[문맥]
당신은 IT-Software 전문 도메인을 가진 데이터 전문가입니다. 주어진 json형태의 데이터에서 필요한 데이터를 추출하거나 요약하는 임무를 맡았습니다.
[Instruction]
Input Data에서 주어진 json 포맷의 텍스트 데이터에서 `indurstry_type`, `job_title`, `job_category`, `job_tasks`, `stacks` key의 value 값을 바탕으로 아래에 주어진 key 값들에 대한 value 값을 채워주세요. 단, 모를 경우 "null"을 넣어주세요. output은 json으로 바로 변환 할 수 있어야합니다.
- `tech_stack`: Input Data를 기반으로 유추하거나 추출한 개발에 사용되는 기술의 모음입니다. 여기에는 프로그래밍 언어, 프레임워크, 데이터베이스 시스템, 서버 인프라, 개발 도구 등 다양한 요소가 포함됩니다. 해당 key의 value는 항상 영단어로 이루어진 list여야 합니다.
- `job_requirements`: Input data를 기반으로 유추한 해당 직무의 요구사항입니다. value 값은 한글로 된 열 개 단어 이하인 문장 또는 단어이며, list 포맷으로 제시됩니다.
- `job_prefer`: Input data를 기반으로 유추한 해당 직무의 우대사항입니다. value 값은 한글로 된 열 개 단어 이하의 문장 또는 단어이며, list 포맷으로 제시됩니다.
- `job_category`: Input Data를 기반으로 유추하거나 추출한 직무(position) 분류 입니다. value 값은 영단어로 이루어진 list여야 합니다.
- `indurstry_type`: Input Data의 직무가 처리하는 산업 분류입니다. value 값은 영단어로 이루어진 list여야 합니다.
[Input Data]
{'indurstry_type': '소프트웨어 자문,개발,공급/컴퓨터주변기기 도소매',
 'site_symbol': 'JK',
 'job_title': '광화문 흥국생명 차세대시스템 구축 Java, BXM 프레임웍, 융자업무 관련 경험자를 모집 합니다. (s01)',
 'job_category': '응용 소프트웨어 개발 및 공급업',
 'crawl_url': 'https://www.jobkorea.co.kr/Recruit/GI_Read/45423573',
 'end_date': '1724598000',
 'required_career': False,
 'job_id': '45423573',
 'company_name': '행복추구 패러다임㈜kcms',
 'resume_required': True,
 'start_date': '1724511600',
 'get_date': Decimal('1724605790'),
 'job_tasks': '광화문 흥국생명 차세대시스템 구축',
 'id': Decimal('2352333735'),
 'stacks': 'JAVA, BXM 프레임웍, 융자업무 관련 경험자'}


 # rocket-punch
 ```json
{
  "dev_stack": [
    "Node.js",
    "TypeScript",
    "NestJS",
    "Java",
    "Spring",
    "MySQL",
    "MongoDB",
    "Kafka",
    "Redis",
    "OpenSearch",
    "Kubernetes",
    "GitOps",
    "Datadog",
    "Grafana",
    "Prometheus",
    "AWS",
    "NCP"
  ],
  "job_requirements": [
    "컴퓨터 공학 관련 전공",
    "웹서버 프레임워크 활용 백엔드 API 개발 경험",
    "Git 사용 가능",
    "산업기능요원 보충역 근무 대상",
    "부산 근무 가능"
  ],
  "job_prefer": [
    "실제 사용 목적 서비스 개발 경험",
    "협업 프로젝트 수행 경험",
    "자동화 테스트 코드 작성 가능"
  ],
  "job_category": [
    "Backend Engineer",
    "Software Engineer",
    "API Developer"
  ],
  "indurstry_type": [
    "LegalTech",
    "FinTech",
    "Software",
    "Technology"
  ]
}
```
