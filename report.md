# Доклад тост за toast 
## Про TOAST
> Слайд 2

__TOAST (The Oversized-Attribute Storage Technique)__ — это механизм PostgreSQL для хранения больших значений полей, которые не помещаются в одну страницу данных.
Зачем нужен TOAST
PostgreSQL использует страницы фиксированного размера (по умолчанию 8 КБ). Одна строка (кортеж) не может занимать несколько страниц + PostgreSQL стремится к тому, чтобы на странице помещалось хотя бы 4 строки. 

> Слайд 3

TOAST решает эту проблему: большие значения (например, длинный text, jsonb, bytea, массивы и т.п.) сжимаются и/или делятся на чанки по 2 КБ и выносятся в специальную TOAST-таблицу.

### Стратегии хранения
> Слайд 4

- plain — TOAST не используется (применяется для заведомо “коротких” типов данных, как integer);
- extended — допускается как сжатие, так и хранение в отдельной TOAST-таблице;
- external — длинные значения хранятся в TOAST-таблице несжатыми;
- main — длинные значения в первую очередь сжимаются, а в TOAST-таблицу попадают только если сжатие не помогло.

Можно задавать для колонки через 
```sql
ALTER TABLE table ALTER COLUMN column SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
```

### Алгоритм
> Слайд 5

Алгоритм выполняется до тех пор, пока строка не перестанет превышать порог:
1. Сначала перебираем атрибуты со стратегиями external и extended, двигаясь от самых длинных к более коротким. Extended-атрибуты сжимаются (если это дает эффект) и, если значение само по себе превосходит четверть страницы, оно сразу же отправляется в TOAST-таблицу. External-атрибуты обрабатываются так же, но не сжимаются.
2. Если после первого прохода версия строки все еще не помещается, отправляем в TOAST-таблицу оставшиеся атрибуты со стратегиями external и extended.
3. Пытаемся сжать атрибуты со стратегией main, оставляя их при этом в табличной странице.
4. main-атрибуты отправляются в TOAST-таблицу.

### TOAST таблица
> Слайд 6

- Для каждой основной таблицы при необходимости создается отдельная, но одна для всех атрибутов, TOAST-таблица (pg_toast_<oid_основной_таблицы>).

|   Column   |  Type   | Description 
|------------|---------|------------
| chunk_id   | oid     | id значения 
| chunk_seq  | integer | номер чанка
| chunk_data | bytea   | данные

- К TOAST-таблице создается индекс (chunk_id, chunk_seq)
- Необходимость определяется наличием в таблице потенциально длинных атрибутов.
- В основной таблице вместо большого значения остаётся маленький TOAST-указатель (pointer), который содержит ссылку на данные в TOAST-таблице + служебную информацию (длины, флаги сжатия и т.д.).

## Предыстория
> Слайд 7

Мы используем увесиситые jsonb поля, чтение стало тормозить, DBA указали на jsonb и TOAST, что они могут быть причиной и предложили разложить на скаляры. Стало интересно как TOAST вляет на перфоманс и правда ли во всех случаях стоит опасаться TOAST

## Решила исследовать 
> Слайд 8

Генерировала данные и раскладывала в бд в 3 схемы: с jsonb extended, jsonb external, скаляры
Сравнивала время вставки, чтения и объем получившихся данных

```scala
final case class Event(
    eventId: UUID,
    jobId: UUID,
    eventType: String,
    occurredAt: Instant,
    source: String,
    level: String,
    body: EventBody
)
sealed trait EventBody
```

```sql
    CREATE TABLE events_scalar (
      event_id     UUID PRIMARY KEY,
      job_id       UUID NOT NULL,
      event_type   TEXT NOT NULL,
      occurred_at  TIMESTAMPTZ NOT NULL,
      source       TEXT NOT NULL,
      level        TEXT NOT NULL
    )
    CREATE TABLE events_jsonb (
      event_id     UUID PRIMARY KEY,
      job_id       UUID NOT NULL,
      event_type   TEXT NOT NULL,
      occurred_at  TIMESTAMPTZ NOT NULL,
      source       TEXT NOT NULL,
      level        TEXT NOT NULL,
      body         JSONB NOT NULL
    )
```

### Результаты
#### EventBody: JobInProgress, JobSuccess
> Слайд 7
```scala
  final case class JobInProgress(
      workerId: String,
      attempt: Int,
      progressPercent: Int
  ) extends EventBody

  final case class JobSuccess(
      durationMs: Long,
      resultSummary: String,
      outputSizeBytes: Long
  ) extends EventBody
```

```sql
    CREATE TABLE events_job_in_progress (
      event_id          UUID PRIMARY KEY REFERENCES events_scalar (event_id),
      worker_id         TEXT NOT NULL,
      attempt           INT NOT NULL,
      progress_percent  INT NOT NULL
    )
    CREATE TABLE events_job_success (
      event_id           UUID PRIMARY KEY REFERENCES events_scalar (event_id),
      duration_ms        BIGINT NOT NULL,
      result_summary     TEXT NOT NULL,
      output_size_bytes  BIGINT NOT NULL
    )
```

```
=== Insert (create) benchmark (n=500000) ===
scalar (3 tables)          8546 ms
events_jsonb (extended)    4890 ms
events_jsonb (external)    4198 ms
=== Storage size ===
events_scalar                heap:    50,97 MB   toast:     0,01 MB   total:    70,20 MB
events_job_in_progress       heap:    17,77 MB   toast:     0,01 MB   total:    28,40 MB
events_job_success           heap:    18,29 MB   toast:     0,01 MB   total:    27,30 MB
scalar total                 heap:    87,03 MB   toast:     0,02 MB   total:   125,91 MB
events_jsonb                 heap:   111,39 MB   toast:     0,01 MB   total:   130,64 MB
events_jsonb_external        heap:   111,39 MB   toast:     0,01 MB   total:   130,64 MB
=== Full table read benchmark (n=500000, avg of 5 runs) ===
events_jsonb (extended)    1260 ms
events_jsonb (external)    1255 ms
scalar (3 tables)          2227 ms
```

#### EventBody: JobInProgress, JobSuccess, JobFailed
> Слайд 8
```scala
  final case class JobFailed(
      attempt: Int,
      errorMessage: String,
      stackTrace: String //генерируем от 40 до 90 строк в stacktrace
  ) extends EventBody
```

```sql
    CREATE TABLE events_job_failed (
      event_id       UUID PRIMARY KEY REFERENCES events_scalar (event_id),
      attempt        INT NOT NULL,
      error_message  TEXT NOT NULL,
      stack_trace    TEXT NOT NULL
    )
```

```
=== Insert (create) benchmark (n=200000) ===
scalar (4 tables)          5901 ms
events_jsonb (extended)    6949 ms
events_jsonb (external)    5076 ms
=== Storage size ===
events_scalar                heap:    20,39 MB   toast:     0,01 MB   total:    28,73 MB
events_job_in_progress       heap:     5,58 MB   toast:     0,01 MB   total:     9,04 MB
events_job_success           heap:     5,77 MB   toast:     0,01 MB   total:     8,41 MB
events_job_failed            heap:    49,85 MB   toast:    15,88 MB   total:    67,44 MB
scalar total                 heap:    81,59 MB   toast:    15,91 MB   total:   113,62 MB
events_jsonb                 heap:    81,27 MB   toast:    26,93 MB   total:   116,55 MB
events_jsonb_external        heap:    40,03 MB   toast:   318,91 MB   total:   367,29 MB
=== Full table read benchmark (n=200000, avg of 5 runs) ===
events_jsonb (extended)    2250 ms
events_jsonb (external)    3152 ms
scalar (4 tables)          1353 ms
```

#### EventBody: JobInProgress, JobSuccess, JobFailed, JobCreated (max params 100)
> Слайд 9
```scala
  final case class JobCreated(
      priority: Int,
      submittedBy: String,
      queueName: String,
      params: List[Param]
  ) extends EventBody
```

```sql
    CREATE TABLE events_job_created (
      event_id      UUID PRIMARY KEY REFERENCES events_scalar (event_id),
      priority      INT NOT NULL,
      submitted_by  TEXT NOT NULL,
      queue_name    TEXT NOT NULL
    )

    CREATE TABLE events_job_created_params (
      event_id  UUID NOT NULL REFERENCES events_job_created (event_id),
      seq       INT NOT NULL,
      name      TEXT NOT NULL,
      value     TEXT NOT NULL,
      PRIMARY KEY (event_id, seq)
    )
```

```
=== Insert (create) benchmark (n=100000) ===
scalar (6 tables)         21002 ms
events_jsonb (extended)   14481 ms
events_jsonb (external)   10081 ms
=== Storage size ===
events_scalar                heap:    10,20 MB   toast:     0,01 MB   total:    14,41 MB
events_job_created           heap:     2,02 MB   toast:     0,01 MB   total:     3,23 MB
events_job_created_params    heap:   232,31 MB   toast:     0,01 MB   total:   313,38 MB
events_job_in_progress       heap:     1,95 MB   toast:     0,01 MB   total:     3,16 MB
events_job_success           heap:     2,02 MB   toast:     0,01 MB   total:     3,09 MB
events_job_failed            heap:    17,47 MB   toast:     5,52 MB   total:    23,61 MB
scalar total                 heap:   265,97 MB   toast:     5,56 MB   total:   360,88 MB
events_jsonb                 heap:    35,51 MB   toast:   234,33 MB   total:   274,05 MB
events_jsonb_external        heap:    21,05 MB   toast:   336,42 MB   total:   361,68 MB
=== Full table read benchmark (n=100000, avg of 5 runs) ===
events_jsonb (extended)    2481 ms
events_jsonb (external)    2411 ms
scalar (6 tables)          3190 ms
```

#### EventBody: JobInProgress, JobSuccess, JobFailed, JobCreated (max params 10000)
> Слайд 10
```
=== Insert (create) benchmark (n=100) ===
scalar (6 tables)         12309 ms
events_jsonb (extended)   10232 ms
events_jsonb (external)    2876 ms
=== Storage size ===
events_scalar                heap:     0,02 MB   toast:     0,01 MB   total:     0,06 MB
events_job_created           heap:     0,01 MB   toast:     0,01 MB   total:     0,03 MB
events_job_created_params    heap:   216,70 MB   toast:     0,01 MB   total:   306,81 MB
events_job_in_progress       heap:     0,01 MB   toast:     0,01 MB   total:     0,03 MB
events_job_success           heap:     0,01 MB   toast:     0,01 MB   total:     0,03 MB
events_job_failed            heap:     0,02 MB   toast:     0,02 MB   total:     0,09 MB
scalar total                 heap:   216,76 MB   toast:     0,06 MB   total:   307,05 MB
events_jsonb                 heap:     0,04 MB   toast:   197,89 MB   total:   197,97 MB
events_jsonb_external        heap:     0,02 MB   toast:   198,00 MB   total:   198,06 MB
events_jsonb (extended)    1074 ms
events_jsonb (external)     993 ms
scalar (6 tables)          1336 ms
```

#### Для моделей EventBody с большим количеством параметров
> Слайд 11
```scala
    final case class JobCreated(
      priority: Int,
      submittedBy: String,
      queueName: String,
      attr1...attr22: String
    )
    final case class JobSuccess(
      durationMs: Long,
      resultSummary: String,
      outputSizeBytes: Long,
      attr1...attr22: String
    )
    final case class JobFailed(
      attempt: Int,
      errorMessage: String,
      stackTrace: String,
      attr1...attr22: String
    )
```

```
=== Insert (create) benchmark (n=100000) ===
scalar (4 tables)             10959 ms
wide_events_jsonb (extended)  12405 ms
wide_events_jsonb (external)  12308 ms
=== Storage size ===
wide_events                  heap:    10,20 MB   toast:     0,01 MB   total:    14,55 MB
wide_job_created             heap:    67,68 MB   toast:    21,40 MB   total:    90,38 MB
wide_job_success             heap:    78,17 MB   toast:    25,05 MB   total:   104,84 MB
wide_job_failed              heap:    49,47 MB   toast:    46,91 MB   total:    97,51 MB
scalar total                 heap:   205,52 MB   toast:    93,36 MB   total:   307,27 MB
wide_events_jsonb            heap:    12,02 MB   toast:   314,34 MB   total:   330,70 MB
wide_events_jsonb_external   heap:    12,02 MB   toast:   453,23 MB   total:   469,59 MB
=== Full table read benchmark (n=100000, avg of 5 runs) ===
wide_events_jsonb (external)   2808 ms
wide_events_jsonb (extended)   2942 ms
scalar (4 tables)              2377 ms
```

## Выводы
> Слайд 12

TOAST не всегда показывает просадку в скорости в сравнении со скалярами, тут имеет значению количество итоговых таблиц в нормализованном варианте и их ширина. В любом случае TOAST нужно иметь ввиду и сразу думать про развитие схемы