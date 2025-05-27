# Classic Scheduler

Предоставляет простой in-process планировщик, способный выполнять задачи
по cron-расписанию, периодически и отложенно. 

## Пример:

```python
from classic.scheduler import Scheduler


def func():
    print('Hello world!')

scheduler = Scheduler()
scheduler.with_delay(1, func)
scheduler.by_period(5, func)
scheduler.by_cron('* * * * *', func)
scheduler.run()

# Hello world!
# Hello world!
# Hello world!
# ...
```

## Многопоточность
У Scheduler есть параметр в конструкторе - workers_num, по умолчанию равный 1.
Этот параметр задает количество потоков в ThreadPool, используемому под капотом.
При значении workers_num = 0 задачи будут выполняться в том же потоке,
что и планировщик, последовательно.

Также у планировщика есть 2 способа запуска - методы .run() и .start().

Метод .run() вводит поток в вечный цикл, в котором происходит управление
задачами по расписанию.

Метод .start() запускает фоновый поток, в котором выполняется .run().
Также планировщик имеет метод .stop(), используя который можно отправить
сообщение об остановке. Также .stop имеет параметры block и timeout, позволяющие
остановить текущий поток в ожидании завершения фонового потока, 
и установить время ожидания остановки.

Метод .join() блокирует текущий поток в ожидании завершения фонового потока

## Интеграция с classic-components
Если используется совместно с classic-components,
то можно использовать декораторы by_period и by_cron на классах с логикой,\
чтобы упростить регистрацию задач:
```python
from classic.components import component
from classic.scheduler import Scheduler, by_period, by_cron


@component
def SomeLogic:
    
    @by_period(1)
    def need_to_call_periodically(self):
        print('Hello')
    
    @by_cron('* * * * *')
    def need_to_call_by_schedule(self):
        print('Hi')


scheduler = Scheduler()
some_logic = SomeLogic(scheduler=scheduler)  # Регистрация произойдет при инстанцировании компонента
scheduler.run()

# Hello
# Hi
```

## Отмена вызова (удаление задачи)

Если расписание задач планируется динамически менять, например, удалить ранее созданную задачу, то при создании задачи в методах необходимо указать атрибут `task_name`. Пример:

```python
from classic.scheduler import Scheduler

scheduler = Scheduler()
scheduler.by_period(1, lambda: print('hello'), task_name='periodic task')
scheduler.cancel('periodic task')
```

Тогда в дальнейшем вы можете отменить запланированные запуски задач через:

```python
scheduler.cancel('periodic task')
```

## Параметризация вызываемой функции

Планировщик может запускать и функции с аргументами. Аргументы передаются через атрибуты `args` и `kwargs`. Отдельно заметим, что вызываемые объекты так же могут быть акторами, как в примере ниже:

```python
from classic.scheduler import Scheduler


def print_sum(a, b):
    print(f'a + b = {a+b}')

    
scheduler = Scheduler()
scheduler.run()
scheduler.by_period(
    2,
    print_sum,
    args=(2, 6),
)
# a + b = 8
# a + b = 8
# a + b = 8
# ...
```

## Типы задач

После создания экземпляра планировщика нужно добавить в него задач. Планирование и выполнение задач начнется только после запуска метода `run()`. Отметим, что добавлять задачи в планировщик можно и после вызова метода `run()`.

Планировщик поддерживает три типа задач:

### Одноразовая отложенная задача

Создается через вызов метода планировщика `with_delay`. Задача будет выполнена ровно один раз. Ее вызов отложен на `delay` указанный либо в секундах в `float` либо в `timedelta`.

### Переодическая задача

Создается через вызов метода планировщика `by_period`. Задача выполнится в первый раз, затем она будет выполняется через период `periodic` заданный либо в секундах в `float` либо в `timedelta`.

### Задача по расписанию

Создается через вызов метода планировщика `by_cron`. Задача вызов который задается в формате `CRON` строкой.
