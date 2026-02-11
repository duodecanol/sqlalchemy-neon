https://neon.com/blog/quicker-serverless-postgres
https://neon.com/blog/serverless-driver-for-postgres
- https://github.com/neondatabase/wsproxy

https://github.com/neondatabase/serverless/blob/main/src/pool.ts

https://raw.githubusercontent.com/neondatabase/serverless/main/src/httpQuery.ts

---


이 neon http는 pgbouncer 를 거치게 되어있어서 순차적 프로토콜을 반드시 유지할 필요는 없다.
만약 상태가 필요해서 순차 프로토콜을 이용하고자 한다면, `transaction` 방식으로 요청을 보내고 처리하게끔 설계되었다.
본 코드의 구현이 `psycopg` 의 래핑이 아니라 자체 프로토콜 구현이라고 한다면, `Lock`을 제거하고 multiplexing이
가능하게끔 구현할수 있지 않은가?

---

Neon 문서를 보면  스트림 식별자를 제공한다고 되어있는데, 실제 응답들을 기록해볼 수 없을까?
또한 Typescript 구현체에서는 연결을 점유하지 않고 async Callback으로 나중에 값을 받게 되어 있다.
이것을 적용할 수 없는 것인가?

https://neon.com/blog/quicker-serverless-postgres
https://github.com/neondatabase/serverless/blob/main/src/pool.ts

---

이제 다음 문서를 읽고 해당 문서에서 언급하는 기술적 목표와 성취를 정리하고, 본 실험에 적용할 수 있는 내용을 찾아라.
https://neon.com/blog/serverless-driver-for-postgres