import time


for i in range(10):
    now_time = time.time()
    print('начало запроса', now_time)
    time.sleep(1)
    print('конец запроса', now_time , time.time(), now_time < time.time() - 1)