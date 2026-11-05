# Classic Scheduler

Простой in-process планировщик: вызывает ваши функции отложенно, периодически
или по cron-расписанию внутри процесса, без внешних сервисов и брокеров.

- Три типа задач: `with_delay` (один раз через отсрочку), `by_period`
  (периодически), `by_cron` (по cron-выражению).
- Задачи выполняются в пуле потоков или в потоке самого планировщика.
- Запуск блокирующий (`run()`) или в фоновом потоке (`start()`).
- Каждая задача отменяется отдельно через возвращённый `ScheduledJob`.

Требования: Python 3.10+, зависимость `croniter` (ставится автоматически).

```
pip install classic-scheduler
```

## Быстрый старт

```python
from classic.scheduler import Scheduler


def ping() -> None:
    print("ping")


if __name__ == "__main__":
    scheduler = Scheduler()
    scheduler.with_delay(5, ping)         # один раз через 5 секунд
    scheduler.by_period(60, ping)         # сразу, затем раз в минуту
    scheduler.by_cron("0 3 * * *", ping)  # каждый день в 03:00
    scheduler.run()  # блокирует поток; Ctrl+C - штатная остановка
```

Задачи можно добавлять и до запуска, и после, из любого потока.
Выполнение начинается только после `run()` или `start()`.

## Типы задач

Все три метода принимают вызываемый объект `job` и необязательные
аргументы для него: `args` (кортеж) и `kwargs` (словарь).

```python
def print_sum(a: int, b: int, prefix: str = "") -> None:
    print(f"{prefix}{a} + {b} = {a + b}")


scheduler.by_period(2, print_sum, args=(2, 6), kwargs={"prefix": "> "})
# > 2 + 6 = 8
# > 2 + 6 = 8
# ...
```

### `with_delay` - одноразовая задача

```python
from datetime import timedelta

scheduler.with_delay(30, send_report)                    # секунды
scheduler.with_delay(timedelta(minutes=5), send_report)  # timedelta
```

Задача выполняется ровно один раз. Отсрочка отсчитывается от момента вызова
метода (даже если планировщик ещё не запущен).

### `by_period` - периодическая задача

```python
scheduler.by_period(60, collect_metrics)
scheduler.by_period(timedelta(hours=1), cleanup)
```

Первый запуск происходит сразу после добавления. Каждый следующий назначается
через `period` после момента, когда предыдущий запуск был отправлен
на выполнение (а не после его завершения).

Если предыдущий запуск ещё не завершился (задача работает дольше периода),
очередной запуск пропускается с предупреждением в лог. Пропущенные запуски
не наверстываются, следующий выполнится через `period`.

### `by_cron` - задача по cron-расписанию

```python
scheduler.by_cron("*/15 * * * *", sync)      # каждые 15 минут
scheduler.by_cron("30 9 * * 1-5", report)    # по будням в 09:30
scheduler.by_cron("* * * * * *", heartbeat)  # каждую секунду (6 полей)
```

Расписание разбирает [croniter](https://pypi.org/project/croniter/):
стандартные 5 полей (минута, час, день месяца, месяц, день недели).
Необязательное 6-е поле **в конце** - секунды.

**Часовой пояс** выбирается так: аргумент `tz` у `by_cron`, иначе `tz`
у `Scheduler`, иначе локальный часовой пояс процесса. Для корректного
перехода на летнее время передавайте `zoneinfo.ZoneInfo`: фиксированные
смещения (`timezone(timedelta(hours=3))`) за переходом не следуют.

```python
from datetime import timezone
from zoneinfo import ZoneInfo

scheduler = Scheduler(tz=ZoneInfo("Europe/Moscow"))  # по умолчанию для cron-задач
scheduler.by_cron("30 9 * * 1-5", report)            # 09:30 по Москве
scheduler.by_cron("0 0 * * *", cleanup, tz=timezone.utc)  # полночь по UTC
```

Как и для `by_period`, если предыдущий запуск ещё идёт, очередной
пропускается с предупреждением.

## Отмена задач

`with_delay`, `by_period` и `by_cron` возвращают `ScheduledJob`.
Его метод `cancel()` отменяет именно эту задачу: две задачи с одной
и той же функцией (или lambda) отменяются независимо.

```python
job = scheduler.by_period(60, collect_metrics)
...
job.cancel()
```

- Отмена асинхронная: команда передаётся планировщику и применяется
  его циклом.
- Уже идущий запуск не прерывается, отменяются только будущие.
- Повторный `cancel()`, вызов после выполнения одноразовой задачи и вызов
  при остановленном планировщике безопасны (в последнем случае отмена
  применится при следующем запуске).

## Запуск и остановка

| Метод | Назначение |
|---|---|
| `run()` | Запускает планировщик в текущем потоке и блокирует его. Возвращает управление после `stop()` или по Ctrl+C (`KeyboardInterrupt` подавляется, остановка штатная). `RuntimeError`, если планировщик уже запущен. |
| `start(daemon=False)` | Запускает планировщик в фоновом потоке и сразу возвращается. Повторный вызов при работающем планировщике ничего не делает. |
| `stop(block=False, timeout=None)` | Останавливает планировщик, запущенный через `run()` или `start()`. Без `block=True` возвращается сразу. |
| `join(timeout=None)` | Ждёт завершения фонового потока (как `threading.Thread.join`). |
| `is_alive()` | `True`, пока планировщик работает - и после `start()`, и во время `run()`. |

Подробности:

- Поток из `start()` по умолчанию **не daemon**: процесс не завершится,
  пока не вызван `stop()`. С `start(daemon=True)` процесс может завершиться
  в любой момент, и выполняющиеся задачи при этом оборвутся.
- `stop()` даёт доработать уже выполняющимся задачам; ещё не начатые задачи
  в пуле отменяются.
- `stop(block=True, timeout=...)` и `join()` ждут только поток, созданный
  через `start()`. Для `run()` они не ждут: `run()` сам вернёт управление
  после остановки. `stop()` для `run()` вызывают из другого потока или
  из самой задачи.
- После остановки планировщик можно запустить снова: запланированные задачи
  сохраняются.

Рекомендуемый шаблон для фонового запуска:

```python
scheduler = Scheduler()
scheduler.start()
try:
    job = scheduler.by_period(10, ping)
    ...  # основная работа приложения
    job.cancel()
finally:
    scheduler.stop(block=True)
```

## Потоки

`Scheduler(workers_num=1)` задаёт число потоков пула, в которых выполняются
задачи.

- `workers_num=1` (по умолчанию) - задачи выполняются по одной; долгая задача
  задерживает остальные.
- `workers_num=N` - до N задач одновременно.
- `workers_num=0` - без пула: задачи выполняются последовательно в потоке
  планировщика, долгая задача задерживает все остальные.

Запуски **одной** задачи не пересекаются: если предыдущий запуск в пуле ещё
идёт, очередной пропускается с предупреждением (для `by_period` и `by_cron`).
Другие задачи это не затрагивает. Добавлять и отменять задачи можно
из любого потока.

## Ошибки и логирование

Исключение в задаче логируется (ERROR с traceback) и не останавливает
планировщик: остальные задачи и последующие запуски этой задачи продолжаются.

Используются именованные логгеры, корневой логгер не затрагивается:

| Логгер | Событие | Уровень |
|---|---|---|
| `classic.scheduler.task` | начало и завершение запуска | DEBUG |
| `classic.scheduler.task` | исключение в задаче | ERROR (с traceback) |
| `classic.scheduler.scheduler` | запуск пропущен, предыдущий ещё идёт | WARNING |

Настройка - стандартным `logging` по имени родителя:

```python
import logging

logging.basicConfig(format="%(asctime)s %(name)s %(levelname)s %(message)s")
logging.getLogger("classic.scheduler").setLevel(logging.DEBUG)  # видеть запуски
```

## Переход с 1.x на 2.0

Критические изменения:

1. **Зависимости `classic-actors` и `classic-components` удалены.** `Scheduler`
   больше не `Actor` и не `Registry`: его нельзя передать в
   `classic.actors.Supervisor`. Декораторы `by_cron`/`by_period` и методы
   `register`/`unregister` удалены: регистрируйте задачи явно.

   ```python
   # 1.x: методы помечались декораторами и регистрировались автоматически
   from classic.components import component
   from classic.scheduler import Scheduler, by_period

   @component
   class Jobs:
       @by_period(60)
       def collect(self): ...

   jobs = Jobs(scheduler=scheduler)  # регистрация при создании

   # 2.0
   jobs = Jobs()
   scheduler.by_period(60, jobs.collect)
   ```

2. **Параметр `task_name` и `Scheduler.cancel(name)` удалены.** Отмена -
   через возвращаемый `ScheduledJob`.

   ```python
   # 1.x
   scheduler.by_period(1, func, task_name="periodic task")
   scheduler.cancel("periodic task")

   # 2.0
   job = scheduler.by_period(1, func)
   job.cancel()
   ```

3. **Cron считается в локальном часовом поясе, а не в UTC.** Чтобы сохранить
   прежнее поведение, передайте UTC:

   ```python
   from datetime import timezone

   # 1.x: "0 9 * * *" - 09:00 UTC; в 2.0 то же самое:
   scheduler = Scheduler(tz=timezone.utc)  # для всех cron-задач
   scheduler.by_cron("0 9 * * *", func, tz=timezone.utc)  # для одной задачи
   ```

4. **Запуск и остановка.** `start()` возвращает `None` (в 1.x возвращал
   идентификатор потока), поток по умолчанию не daemon. Появились
   `stop(block, timeout)` и `join(timeout)`; `stop()` теперь работает и для
   `run()`, `is_alive()` истинно и при `run()`. Планировщик можно запускать
   повторно после `stop()`.

   ```python
   # 1.x: daemon-поток, процесс мог завершиться посреди задачи
   scheduler.start()

   # 2.0: прежнее поведение
   scheduler.start(daemon=True)
   # 2.0: рекомендуемое - корректная остановка
   scheduler.stop(block=True)
   ```

5. **Перекрывающиеся запуски пропускаются.** Если предыдущий запуск
   `by_period`/`by_cron`-задачи ещё идёт, очередной пропускается
   с предупреждением (раньше при `workers_num > 1` они выполнялись параллельно,
   при `workers_num=1` копились в очереди пула).

6. **Логирование** перенесено из корневого логгера в `classic.scheduler`;
   начало и завершение запуска теперь DEBUG (раньше INFO).

   ```python
   # 2.0: вернуть прежнюю видимость запусков
   logging.getLogger("classic.scheduler").setLevel(logging.DEBUG)
   ```

## Разработка

```
uv sync --group dev
uv run pytest
uv run ruff check
uv run ruff format --check
uv run pyright
uv run vulture
```

Мутационное тестирование `mutmut` устанавливается только вне Windows
и запускается на Linux/WSL: `uv run mutmut run` (конфигурация - в секции
`[tool.mutmut]` файла `pyproject.toml`).
