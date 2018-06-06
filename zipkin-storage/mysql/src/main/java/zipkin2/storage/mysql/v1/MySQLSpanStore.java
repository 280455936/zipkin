/*
 * Copyright 2015-2018 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.mysql.v1;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.jooq.Condition;
import org.jooq.Cursor;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Row3;
import org.jooq.SelectConditionStep;
import org.jooq.SelectField;
import org.jooq.SelectOffsetStep;
import org.jooq.TableField;
import org.jooq.TableOnConditionStep;
import zipkin2.storage.mysql.v1.internal.generated.tables.ZipkinAnnotations;
import zipkin2.Call;
import zipkin2.DependencyLink;
import zipkin2.Endpoint;
import zipkin2.Span;
import zipkin2.internal.DependencyLinker;
import zipkin2.internal.Nullable;
import zipkin2.storage.QueryRequest;
import zipkin2.storage.SpanStore;
import zipkin2.v1.V1BinaryAnnotation;
import zipkin2.v1.V1Span;
import zipkin2.v1.V1SpanConverter;

import static java.util.stream.Collectors.groupingBy;
import static org.jooq.impl.DSL.row;
import static zipkin2.storage.mysql.v1.MySQLSpanConsumer.UTF_8;
import static zipkin2.storage.mysql.v1.internal.generated.tables.ZipkinAnnotations.ZIPKIN_ANNOTATIONS;
import static zipkin2.storage.mysql.v1.internal.generated.tables.ZipkinDependencies.ZIPKIN_DEPENDENCIES;
import static zipkin2.storage.mysql.v1.internal.generated.tables.ZipkinSpans.ZIPKIN_SPANS;
import static zipkin2.internal.DateUtil.getDays;
import static zipkin2.internal.HexCodec.lowerHexToUnsignedLong;

final class MySQLSpanStore implements SpanStore {
  static final Endpoint EMPTY_ENDPOINT = Endpoint.newBuilder().build();

  private final DataSource datasource;
  private final DSLContexts context;
  private final Schema schema;
  private final boolean strictTraceId;
  private final GroupByTraceId groupByTraceId;

  MySQLSpanStore(DataSource datasource, DSLContexts context, Schema schema, boolean strictTraceId) {
    this.datasource = datasource;
    this.context = context;
    this.schema = schema;
    this.strictTraceId = strictTraceId;
    this.groupByTraceId = new GroupByTraceId(strictTraceId);
  }

  static Endpoint endpoint(Record a) {
    Endpoint.Builder result =
        Endpoint.newBuilder()
            .serviceName(a.getValue(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME))
            .port(maybeGet(a, ZIPKIN_ANNOTATIONS.ENDPOINT_PORT, (short) 0));
    int ipv4 = a.getValue(ZIPKIN_ANNOTATIONS.ENDPOINT_IPV4);
    if (ipv4 != 0) {
      result.parseIp( // allocation is ok here as Endpoint.ipv4Bytes would anyway
          new byte[] {
            (byte) (ipv4 >> 24 & 0xff),
            (byte) (ipv4 >> 16 & 0xff),
            (byte) (ipv4 >> 8 & 0xff),
            (byte) (ipv4 & 0xff)
          });
    }
    result.parseIp(maybeGet(a, ZIPKIN_ANNOTATIONS.ENDPOINT_IPV6, null));
    Endpoint ep = result.build();
    return !EMPTY_ENDPOINT.equals(ep) ? ep : null;
  }

  SelectOffsetStep<? extends Record> toTraceIdQuery(DSLContext context, QueryRequest request) {
    long endTs = request.endTs() * 1000;

    TableOnConditionStep<?> table =
        ZIPKIN_SPANS.join(ZIPKIN_ANNOTATIONS).on(schema.joinCondition(ZIPKIN_ANNOTATIONS));

    int i = 0;
    for (Map.Entry<String, String> kv : request.annotationQuery().entrySet()) {
      ZipkinAnnotations aTable = ZIPKIN_ANNOTATIONS.as("a" + i++);
      if (kv.getValue().isEmpty()) {
        table =
            maybeOnService(
                table
                    .join(aTable)
                    .on(schema.joinCondition(aTable))
                    .and(aTable.A_KEY.eq(kv.getKey())),
                aTable,
                request.serviceName());
      } else {
        table =
            maybeOnService(
                table
                    .join(aTable)
                    .on(schema.joinCondition(aTable))
                    .and(aTable.A_TYPE.eq(V1BinaryAnnotation.TYPE_STRING))
                    .and(aTable.A_KEY.eq(kv.getKey()))
                    .and(aTable.A_VALUE.eq(kv.getValue().getBytes(UTF_8))),
                aTable,
                request.serviceName());
      }
    }

    List<SelectField<?>> distinctFields = new ArrayList<>(schema.spanIdFields);
    distinctFields.add(ZIPKIN_SPANS.START_TS.max());
    SelectConditionStep<Record> dsl =
        context
            .selectDistinct(distinctFields)
            .from(table)
            .where(ZIPKIN_SPANS.START_TS.between(endTs - request.lookback() * 1000, endTs));

    if (request.serviceName() != null) {
      dsl.and(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME.eq(request.serviceName()));
    }

    if (request.spanName() != null) {
      dsl.and(ZIPKIN_SPANS.NAME.eq(request.spanName()));
    }

    if (request.minDuration() != null && request.maxDuration() != null) {
      dsl.and(ZIPKIN_SPANS.DURATION.between(request.minDuration(), request.maxDuration()));
    } else if (request.minDuration() != null) {
      dsl.and(ZIPKIN_SPANS.DURATION.greaterOrEqual(request.minDuration()));
    }
    return dsl.groupBy(schema.spanIdFields)
        .orderBy(ZIPKIN_SPANS.START_TS.max().desc())
        .limit(request.limit());
  }

  static TableOnConditionStep<?> maybeOnService(
      TableOnConditionStep<Record> table, ZipkinAnnotations aTable, String serviceName) {
    if (serviceName == null) return table;
    return table.and(aTable.ENDPOINT_SERVICE_NAME.eq(serviceName));
  }

  @Override
  public Call<List<List<Span>>> getTraces(QueryRequest request) {
    List<Span> result = getTraces(request, 0L, 0L);
    return Call.create(groupByTraceId.map(result));
  }

  List<Span> getTraces(@Nullable QueryRequest request, long traceIdHigh, long traceIdLow) {
    if (traceIdHigh != 0L && !strictTraceId) traceIdHigh = 0L;
    final Map<Pair, List<V1Span.Builder>> spansWithoutAnnotations;
    final Map<Row3<Long, Long, Long>, List<Record>> dbAnnotations;
    try (Connection conn = datasource.getConnection()) {
      Condition traceIdCondition =
          request != null
              ? schema.spanTraceIdCondition(toTraceIdQuery(context.get(conn), request))
              : schema.spanTraceIdCondition(traceIdHigh, traceIdLow);

      spansWithoutAnnotations =
          context
              .get(conn)
              .select(schema.spanFields)
              .from(ZIPKIN_SPANS)
              .where(traceIdCondition)
              .stream()
              .map(
                  r ->
                      V1Span.newBuilder()
                          .traceIdHigh(maybeGet(r, ZIPKIN_SPANS.TRACE_ID_HIGH, 0L))
                          .traceId(r.getValue(ZIPKIN_SPANS.TRACE_ID))
                          .name(r.getValue(ZIPKIN_SPANS.NAME))
                          .id(r.getValue(ZIPKIN_SPANS.ID))
                          .parentId(maybeGet(r, ZIPKIN_SPANS.PARENT_ID, 0L))
                          .timestamp(maybeGet(r, ZIPKIN_SPANS.START_TS, 0L))
                          .duration(maybeGet(r, ZIPKIN_SPANS.DURATION, 0L))
                          .debug(r.getValue(ZIPKIN_SPANS.DEBUG)))
              .collect(
                  groupingBy(
                      s -> new Pair(s.traceIdHigh(), s.traceId()),
                      LinkedHashMap::new,
                      Collectors.toList()));

      dbAnnotations =
          context
              .get(conn)
              .select(schema.annotationFields)
              .from(ZIPKIN_ANNOTATIONS)
              .where(schema.annotationsTraceIdCondition(spansWithoutAnnotations.keySet()))
              .orderBy(ZIPKIN_ANNOTATIONS.A_TIMESTAMP.asc(), ZIPKIN_ANNOTATIONS.A_KEY.asc())
              .stream()
              .collect(
                  groupingBy(
                      (Record a) ->
                          row(
                              maybeGet(a, ZIPKIN_ANNOTATIONS.TRACE_ID_HIGH, 0L),
                              a.getValue(ZIPKIN_ANNOTATIONS.TRACE_ID),
                              a.getValue(ZIPKIN_ANNOTATIONS.SPAN_ID)),
                      LinkedHashMap::new,
                      Collectors.<Record>toList())); // LinkedHashMap preserves order while grouping
    } catch (SQLException e) {
      throw new RuntimeException("Error querying for " + request + ": " + e.getMessage());
    }

    V1SpanConverter converter = V1SpanConverter.create();
    List<Span> allSpans = new ArrayList<>(spansWithoutAnnotations.size());
    for (List<V1Span.Builder> spans : spansWithoutAnnotations.values()) {
      for (V1Span.Builder span : spans) {
        Row3<Long, Long, Long> key = row(span.traceIdHigh(), span.traceId(), span.id());

        if (dbAnnotations.containsKey(key)) {
          for (Record a : dbAnnotations.get(key)) {
            Endpoint endpoint = endpoint(a);
            int type = a.getValue(ZIPKIN_ANNOTATIONS.A_TYPE);
            if (type == -1) {
              span.addAnnotation(
                  a.getValue(ZIPKIN_ANNOTATIONS.A_TIMESTAMP),
                  a.getValue(ZIPKIN_ANNOTATIONS.A_KEY),
                  endpoint);
            } else {
              // TODO: all values!
              switch (type) {
                case V1BinaryAnnotation.TYPE_STRING:
                  span.addBinaryAnnotation(
                      a.getValue(ZIPKIN_ANNOTATIONS.A_KEY),
                      new String(a.getValue(ZIPKIN_ANNOTATIONS.A_VALUE), UTF_8),
                      endpoint);
                  break;
                case V1BinaryAnnotation.TYPE_BOOLEAN:
                  byte[] value = a.getValue(ZIPKIN_ANNOTATIONS.A_VALUE);
                  if (value.length == 1 && value[0] == 1) { // Address
                    span.addBinaryAnnotation(a.getValue(ZIPKIN_ANNOTATIONS.A_KEY), endpoint);
                  }
                  break;
                default:
                  // log.fine unsupported
              }
            }
          }
        }
        converter.convert(span.build(), allSpans);
      }
    }
    return allSpans;
  }

  /** returns the default value if the column doesn't exist or the result was null */
  static <T> T maybeGet(Record record, TableField<Record, T> field, T defaultValue) {
    if (record.fieldsRow().indexOf(field) < 0) {
      return defaultValue;
    } else {
      T result = record.get(field);
      return result != null ? result : defaultValue;
    }
  }

  @Override
  public Call<List<Span>> getTrace(String hexTraceId) {
    // make sure we have a 16 or 32 character trace ID
    hexTraceId = Span.normalizeTraceId(hexTraceId);
    long traceIdHigh = hexTraceId.length() == 32 ? lowerHexToUnsignedLong(hexTraceId, 0) : 0L;
    long traceId = lowerHexToUnsignedLong(hexTraceId);
    List<Span> result = getTraces(null, traceIdHigh, traceId);
    return Call.create(result.isEmpty() ? null : result);
  }

  @Override
  public Call<List<String>> getServiceNames() {
    List<String> result;
    try (Connection conn = datasource.getConnection()) {
      result =
          context
              .get(conn)
              .selectDistinct(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME)
              .from(ZIPKIN_ANNOTATIONS)
              .where(
                  ZIPKIN_ANNOTATIONS
                      .ENDPOINT_SERVICE_NAME
                      .isNotNull()
                      .and(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME.ne("")))
              .fetch(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME);
    } catch (SQLException e) {
      throw new RuntimeException("Error querying for " + e + ": " + e.getMessage());
    }
    return Call.create(result);
  }

  @Override
  public Call<List<String>> getSpanNames(String serviceName) {
    if (serviceName == null) return Call.emptyList();
    serviceName = serviceName.toLowerCase(Locale.ROOT); // service names are always lowercase!
    List<String> result;
    try (Connection conn = datasource.getConnection()) {
      result =
          context
              .get(conn)
              .selectDistinct(ZIPKIN_SPANS.NAME)
              .from(ZIPKIN_SPANS)
              .join(ZIPKIN_ANNOTATIONS)
              .on(schema.joinCondition(ZIPKIN_ANNOTATIONS))
              .where(ZIPKIN_ANNOTATIONS.ENDPOINT_SERVICE_NAME.eq(serviceName))
              .orderBy(ZIPKIN_SPANS.NAME)
              .fetch(ZIPKIN_SPANS.NAME);
    } catch (SQLException e) {
      throw new RuntimeException("Error querying for " + serviceName + ": " + e.getMessage());
    }
    return Call.create(result);
  }

  @Override
  public Call<List<DependencyLink>> getDependencies(long endTs, long lookback) {
    List<DependencyLink> result;
    try (Connection conn = datasource.getConnection()) {
      if (schema.hasPreAggregatedDependencies) {
        List<Date> days = getDays(endTs, lookback);
        List<DependencyLink> unmerged =
            context
                .get(conn)
                .select(schema.dependencyLinkFields)
                .from(ZIPKIN_DEPENDENCIES)
                .where(ZIPKIN_DEPENDENCIES.DAY.in(days))
                .fetch(
                    (Record l) ->
                        DependencyLink.newBuilder()
                            .parent(l.get(ZIPKIN_DEPENDENCIES.PARENT))
                            .child(l.get(ZIPKIN_DEPENDENCIES.CHILD))
                            .callCount(l.get(ZIPKIN_DEPENDENCIES.CALL_COUNT))
                            .errorCount(maybeGet(l, ZIPKIN_DEPENDENCIES.ERROR_COUNT, 0L))
                            .build());
        result = DependencyLinker.merge(unmerged);
      } else {
        result = aggregateDependencies(endTs, lookback, conn);
      }
    } catch (SQLException e) {
      throw new RuntimeException(
          "Error querying dependencies for endTs "
              + endTs
              + " and lookback "
              + lookback
              + ": "
              + e.getMessage());
    }
    return Call.create(result);
  }

  List<DependencyLink> aggregateDependencies(long endTs, @Nullable Long lookback, Connection conn) {
    endTs = endTs * 1000;
    // Lazy fetching the cursor prevents us from buffering the whole dataset in memory.
    Cursor<Record> cursor =
        context
            .get(conn)
            .selectDistinct(schema.dependencyLinkerFields)
            // left joining allows us to keep a mapping of all span ids, not just ones that have
            // special annotations. We need all span ids to reconstruct the trace tree. We need
            // the whole trace tree so that we can accurately skip local spans.
            .from(
                ZIPKIN_SPANS
                    .leftJoin(ZIPKIN_ANNOTATIONS)
                    // NOTE: we are intentionally grouping only on the low-bits of trace id. This
                    // buys time
                    // for applications to upgrade to 128-bit instrumentation.
                    .on(
                        ZIPKIN_SPANS
                            .TRACE_ID
                            .eq(ZIPKIN_ANNOTATIONS.TRACE_ID)
                            .and(ZIPKIN_SPANS.ID.eq(ZIPKIN_ANNOTATIONS.SPAN_ID)))
                    .and(ZIPKIN_ANNOTATIONS.A_KEY.in("lc", "cs", "ca", "sr", "sa", "error")))
            .where(
                lookback == null
                    ? ZIPKIN_SPANS.START_TS.lessOrEqual(endTs)
                    : ZIPKIN_SPANS.START_TS.between(endTs - lookback * 1000, endTs))
            // Grouping so that later code knows when a span or trace is finished.
            .groupBy(schema.dependencyLinkerGroupByFields)
            .fetchLazy();

    Iterator<Iterator<Span>> traces =
        new DependencyLinkV2SpanIterator.ByTraceId(cursor.iterator(), schema.hasTraceIdHigh);

    if (!traces.hasNext()) return Collections.emptyList();

    DependencyLinker linker = new DependencyLinker();

    while (traces.hasNext()) {
      linker.putTrace(traces.next());
    }

    return linker.link();
  }

  // TODO(adriancole): at some later point we can refactor this out
  static class GroupByTraceId implements Call.Mapper<List<Span>, List<List<Span>>> {
    final boolean strictTraceId;

    GroupByTraceId(boolean strictTraceId) {
      this.strictTraceId = strictTraceId;
    }

    @Override
    public List<List<Span>> map(List<Span> input) {
      if (input.isEmpty()) return Collections.emptyList();

      Map<String, List<Span>> groupedByTraceId = new LinkedHashMap<>();
      for (Span span : input) {
        String traceId =
            strictTraceId || span.traceId().length() == 16
                ? span.traceId()
                : span.traceId().substring(16);
        if (!groupedByTraceId.containsKey(traceId)) {
          groupedByTraceId.put(traceId, new ArrayList<>());
        }
        groupedByTraceId.get(traceId).add(span);
      }
      return new ArrayList<>(groupedByTraceId.values());
    }

    @Override
    public String toString() {
      return "GroupByTraceId{strictTraceId=" + strictTraceId + "}";
    }
  }
}
