```log
======================================================================
GEVENTHTTPCLIENT CONCEPT VERIFICATION
======================================================================
Target URL: https://httpbin.org/get
Number of requests: 10
Concurrency setting: 5

======================================================================
TEST 1: requests library - Synchronous baseline
======================================================================
  Before: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True
    Request 1: 200 in 1.720s
    Request 2: 200 in 0.761s
    Request 3: 200 in 0.879s
    Request 4: 200 in 0.737s
    Request 5: 200 in 0.750s
    Request 6: 200 in 0.789s
    Request 7: 200 in 1.515s
    Request 8: 200 in 0.758s
    Request 9: 200 in 0.853s
    Request 10: 200 in 0.822s
  [10 sequential requests] Elapsed: 9.584s
  Average per request: 0.958s
  After: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True

======================================================================
TEST 2: geventhttpclient - Synchronous (single greenlet)
======================================================================
  Before: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True
    Request 1: 200 in 0.751s, body_len=252
    Request 2: 200 in 0.217s, body_len=252
    Request 3: 200 in 0.187s, body_len=252
    Request 4: 200 in 0.187s, body_len=252
    Request 5: 200 in 0.187s, body_len=252
    Request 6: 200 in 0.187s, body_len=252
    Request 7: 200 in 0.189s, body_len=252
    Request 8: 200 in 0.335s, body_len=252
    Request 9: 200 in 0.188s, body_len=252
    Request 10: 200 in 0.188s, body_len=252
  [10 sequential requests] Elapsed: 2.616s
  Average per request: 0.262s
  After: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True

======================================================================
TEST 3: greenlet.getcurrent() ONLY - No spawn/pool
======================================================================
HYPOTHESIS: NO parallelism - getcurrent() doesn't create greenlets
  Before: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True
    Request 1: 200 in 0.733s, greenlet_id=127230909118912
    Request 2: 200 in 0.210s, greenlet_id=127230909118912
    Request 3: 200 in 0.185s, greenlet_id=127230909118912
    Request 4: 200 in 0.692s, greenlet_id=127230909118912
    Request 5: 200 in 0.185s, greenlet_id=127230909118912
    Request 6: 200 in 0.196s, greenlet_id=127230909118912
    Request 7: 200 in 0.421s, greenlet_id=127230909118912
    Request 8: 200 in 0.246s, greenlet_id=127230909118912
    Request 9: 200 in 0.186s, greenlet_id=127230909118912
    Request 10: 200 in 0.505s, greenlet_id=127230909118912
  [10 requests with getcurrent() only] Elapsed: 3.559s

  Unique greenlets used: 1
  Average per request: 0.356s
  RESULT: SEQUENTIAL (as expected)
  After: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True

======================================================================
TEST 4: gevent.spawn - True parallel execution
======================================================================
EXPECTED: Parallel execution - multiple greenlets doing concurrent I/O
  Before: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True
    Request 4: 200 in 0.738s, greenlet_id=127230901624544
    Request 3: 200 in 0.752s, greenlet_id=127230901624064
    Request 5: 200 in 0.764s, greenlet_id=127230901623744
    Request 2: 200 in 0.776s, greenlet_id=127230901365440
    Request 1: 200 in 0.869s, greenlet_id=127230901365120
    Request 9: 200 in 0.971s, greenlet_id=127230901625024
    Request 7: 200 in 1.021s, greenlet_id=127230901623904
    Request 10: 200 in 1.112s, greenlet_id=127230901625184
    Request 6: 200 in 1.154s, greenlet_id=127230901624384
    Request 8: 200 in 1.216s, greenlet_id=127230901624864
  [10 parallel requests with gevent.spawn] Elapsed: 1.217s

  Unique greenlets used: 10
  Average per request: 0.937s
  RESULT: PARALLEL (as expected)
  After: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True

======================================================================
TEST 5: gevent.Pool - Controlled parallel execution
======================================================================
EXPECTED: 5 concurrent connections
  Before: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True
    Request 4: 200 in 0.750s, greenlet_id=127230901624864
    Request 2: 200 in 0.757s, greenlet_id=127230901365440
    Request 1: 200 in 0.785s, greenlet_id=127230901367520
    Request 3: 200 in 0.922s, greenlet_id=127230917506560
    Request 7: 200 in 0.190s, greenlet_id=127230901626944
    Request 8: 200 in 0.196s, greenlet_id=127230901625184
    Request 5: 200 in 0.984s, greenlet_id=127230901624064
    Request 9: 200 in 0.322s, greenlet_id=127230901625024
    Request 6: 200 in 0.837s, greenlet_id=127230901624544
    Request 10: 200 in 0.677s, greenlet_id=127230901623904
  [10 requests via Pool(size=5)] Elapsed: 1.624s

  Unique greenlets used: 10
  Average per request: 0.642s
  After: Current greenlet: <greenlet.greenlet object at 0x73b7412479c0 (otid=0x73b7413bfd80) current active started main>, id=127230909118912, is_main=True

======================================================================
SUMMARY
======================================================================
Test                        Total Time    Avg/Req
--------------------------------------------------
requests_sync                  9.584s      0.958s
gevent_sync                    2.616s      0.262s
getcurrent_only                3.559s      0.356s
spawn_parallel                 1.217s      0.937s
pool_parallel                  1.624s      0.642s

======================================================================
CONCLUSIONS
======================================================================
```

1. greenlet.getcurrent() ALONE does NOT provide parallelism
   - It only returns a reference to the current greenlet
   - Without gevent.spawn() or Pool, code runs sequentially

2. gevent.spawn() or Pool is REQUIRED for concurrency
   - spawn() creates new greenlets that can switch during I/O
   - Pool provides controlled concurrency with worker limits

3. geventhttpclient's connection pooling helps performance
   - Connection reuse reduces overhead
   - But true parallelism still requires multiple greenlets

4. Sync performance comparison:
   - geventhttpclient is faster than requests due to C parser
   - But without greenlet spawning, both are sequential

## 실험 결과 요약
실험 코드가 experiments/gevent_http_experiment.py에 생성되었습니다.

### 핵심 결과


| 테스트 |	총 | 시간 | 	병렬성 |
|---|---|---|---|
| requests (baseline ) |	9.584s |	순차 |
| geventhttpclient sync |	2.616s |	순차 |
| getcurrent() only |	3.559s |	순차 | (greenlet 1개) |
| gevent.spawn |	1.217s	| 병렬 | (greenlet 10개)
| gevent.Pool |	1.624s	| 병렬 | (greenlet 10개)

### 검증 결론

- Sync 코드 HTTP GET 성능
  - geventhttpclient가 requests 대비 약 3.6배 빠름 (커넥션 재사용 + C 파서 효과)
- greenlet.getcurrent()만으로 병렬성 유지 여부: ❌ 불가능
  - getcurrent()는 현재 실행 중인 greenlet 참조만 반환
  - 새로운 greenlet을 생성하지 않으므로 모든 요청이 main greenlet에서 순차 실행
  - 실험에서 Unique greenlets used: 1 확인
- 진정한 병렬성을 위해서는 gevent.spawn() 또는 Pool이 필수

- spawn/Pool 사용 시 각 요청이 별도 greenlet에서 실행
- I/O 블로킹 시점에 cooperative switching 발생
- 10개 요청 병렬 처리 시 총 시간 1.2초 (순차 대비 ~3배 빠름)