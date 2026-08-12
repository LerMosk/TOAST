# Доклад тост за toast 
## Про TOAST
> Слайд 1

__TOAST (The Oversized-Attribute Storage Technique)__ — это механизм PostgreSQL для хранения больших значений полей, которые не помещаются в одну страницу данных.
Зачем нужен TOAST
PostgreSQL использует страницы фиксированного размера (по умолчанию 8 КБ). Одна строка (кортеж) не может занимать несколько страниц + PostgreSQL стремится к тому, чтобы на странице помещалось хотя бы 4 строки. TOAST решает эту проблему: большие значения (например, длинный text, jsonb, bytea, массивы и т.п.) сжимаются и/или делятся на чанки по 2 КБ и выносятся в специальную TOAST-таблицу.

### Стратегии хранения
> Слайд 2

- plain — TOAST не используется (применяется для заведомо “коротких” типов данных, как integer);
- extended — допускается как сжатие, так и хранение в отдельной TOAST-таблице;
- external — длинные значения хранятся в TOAST-таблице несжатыми;
- main — длинные значения в первую очередь сжимаются, а в TOAST-таблицу попадают только если сжатие не помогло.

Можно задавать для колонки через 
```sql
ALTER TABLE table ALTER COLUMN column SET STORAGE { PLAIN | EXTERNAL | EXTENDED | MAIN }
```

### Алгоритм
> Слайд 3

Алгоритм выполняется до тех пор, пока строка не перестанет превышать порог:
1. Сначала перебираем атрибуты со стратегиями external и extended, двигаясь от самых длинных к более коротким. Extended-атрибуты сжимаются (если это дает эффект) и, если значение само по себе превосходит четверть страницы, оно сразу же отправляется в TOAST-таблицу. External-атрибуты обрабатываются так же, но не сжимаются.
2. Если после первого прохода версия строки все еще не помещается, отправляем в TOAST-таблицу оставшиеся атрибуты со стратегиями external и extended.
3. Пытаемся сжать атрибуты со стратегией main, оставляя их при этом в табличной странице.
4. main-атрибуты отправляются в TOAST-таблицу.

### TOAST таблица
> Слайд 4: картинка с визуализацией

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
> Слайд 5

Мы используем увесиситые jsonb поля, чтение стало тормозить, DBA указали на jsonb и TOAST, что они могут быть причиной и предложили разложить на скаляры. Стало интересно как TOAST вляет на перфоманс и правда ли во всех случаях стоит опасаться TOAST

## Решила исследовать 
> Слайд 6

Генерироавла данные и раскладывала в бд в 2 схемы: с jsob и со скалярами
Сравнивала время вставки, чтения и объем получишихся данных

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
=== Insert (create) benchmark (n=100000) ===
scalar (3 tables)        1599 ms
events_jsonb              815 ms
=== Storage size ===
events_scalar             heap:    10,20 MB   toast:     0,01 MB   total:    14,28 MB
events_job_in_progress    heap:     3,56 MB   toast:     0,01 MB   total:     5,77 MB
events_job_success        heap:     3,66 MB   toast:     0,01 MB   total:     5,63 MB
scalar total              heap:    17,42 MB   toast:     0,02 MB   total:    25,67 MB
events_jsonb              heap:    22,28 MB   toast:     0,01 MB   total:    26,37 MB
=== Full table read benchmark (n=100000, median of 5 runs) ===
scalar (3 tables)         204 ms
events_jsonb              275 ms
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
=== Insert (create) benchmark (n=100000) ===
scalar (4 tables)        2863 ms
events_jsonb             2399 ms
=== Storage size ===
events_scalar             heap:    10,20 MB   toast:     0,01 MB   total:    14,33 MB
events_job_in_progress    heap:     2,80 MB   toast:     0,01 MB   total:     4,57 MB
events_job_success        heap:     2,89 MB   toast:     0,01 MB   total:     4,21 MB
events_job_failed         heap:    24,77 MB   toast:     7,95 MB   total:    33,59 MB
scalar total              heap:    40,66 MB   toast:     7,97 MB   total:    56,70 MB
events_jsonb              heap:    40,54 MB   toast:    13,38 MB   total:    58,04 MB
=== Full table read benchmark (n=100000, median of 5 runs) ===
scalar (4 tables)         487 ms
events_jsonb             1095 ms
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
scalar (6 tables)       15114 ms
events_jsonb             6604 ms
=== Storage size ===
events_scalar             heap:    10,20 MB   toast:     0,01 MB   total:    14,48 MB
events_job_created        heap:     2,02 MB   toast:     0,01 MB   total:     3,22 MB
events_job_created_params heap:   232,31 MB   toast:     0,01 MB   total:   313,48 MB
events_job_in_progress    heap:     1,95 MB   toast:     0,01 MB   total:     3,19 MB
events_job_success        heap:     2,02 MB   toast:     0,01 MB   total:     3,13 MB
events_job_failed         heap:    17,47 MB   toast:     5,52 MB   total:    23,60 MB
scalar total              heap:   265,97 MB   toast:     5,55 MB   total:   361,10 MB
events_jsonb              heap:    35,51 MB   toast:   234,33 MB   total:   274,12 MB
=== Full table read benchmark (n=100000, median of 5 runs) ===
scalar (6 tables)        2297 ms
events_jsonb             2120 ms
```

#### EventBody: JobInProgress, JobSuccess, JobFailed, JobCreated (max params 10000)
> Слайд 10
```
=== Insert (create) benchmark (n=100) ===
scalar (6 tables)       11762 ms
events_jsonb             4444 ms
=== Storage size ===
events_scalar             heap:     0,02 MB   toast:     0,01 MB   total:     0,06 MB
events_job_created        heap:     0,01 MB   toast:     0,01 MB   total:     0,03 MB
events_job_created_params heap:   216,70 MB   toast:     0,01 MB   total:   305,25 MB
events_job_in_progress    heap:     0,01 MB   toast:     0,01 MB   total:     0,03 MB
events_job_success        heap:     0,01 MB   toast:     0,01 MB   total:     0,03 MB
events_job_failed         heap:     0,02 MB   toast:     0,02 MB   total:     0,09 MB
scalar total              heap:   216,76 MB   toast:     0,06 MB   total:   305,49 MB
events_jsonb              heap:     0,04 MB   toast:   197,89 MB   total:   197,97 MB
=== Full table read benchmark (n=100, median of 5 runs) ===
scalar (6 tables)        2532 ms
events_jsonb              957 ms
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
scalar (4 tables)        5829 ms
wide_events_jsonb        8058 ms
=== Storage size ===
wide_events               heap:    10,20 MB   toast:     0,01 MB   total:    14,34 MB
wide_job_created          heap:    67,68 MB   toast:    21,39 MB   total:    90,35 MB
wide_job_success          heap:    78,17 MB   toast:    25,05 MB   total:   104,83 MB
wide_job_failed           heap:    49,47 MB   toast:    46,91 MB   total:    97,49 MB
scalar total              heap:   205,52 MB   toast:    93,35 MB   total:   307,02 MB
wide_events_jsonb         heap:    12,02 MB   toast:   314,34 MB   total:   330,50 MB
=== Full table read benchmark (n=100000, median of 5 runs) ===
scalar (4 tables)        1587 ms
wide_events_jsonb        2774 ms
```

## Выводы
> Слайд 12

TOAST не всегда показывает просадку в скорости в сравнении со скалярами, тут имеет значению количество итоговых таблиц в нормализованном варианте и их ширина. В любом случае TOAST нужно иметь ввиду и сразу думать про развитие схемы