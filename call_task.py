from test_worker import add
import random

if __name__ == "__main__":
    r = add.delay(random.randint(-10, 10), random.randint(-10, 10))
    print(r)

    r2 = add.delay(random.randint(0, 100), random.randint(-100, 0))
    print(r2)

    print(r.get(timeout=30))
    print(r2.get(timeout=30))
