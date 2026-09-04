package com.comet.opik.domain.evaluators;

import com.comet.opik.api.evaluators.AutomationRuleEvaluatorTraceThreadLlmAsJudge;
import com.comet.opik.api.evaluators.AutomationRuleEvaluatorType;
import com.comet.opik.api.events.TraceThreadToScoreLlmAsJudge;
import com.comet.opik.api.resources.utils.RedisContainerUtils;
import com.comet.opik.infrastructure.OnlineScoringConfig;
import com.comet.opik.infrastructure.ServiceTogglesConfig;
import com.comet.opik.infrastructure.redis.RedisStreamCodec;
import com.comet.opik.podam.PodamFactoryUtils;
import com.redis.testcontainers.RedisContainer;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.redisson.Redisson;
import org.redisson.api.RStreamReactive;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.api.stream.StreamMessageId;
import org.redisson.config.Config;
import uk.co.jemos.podam.api.PodamFactory;

import java.util.List;
import java.util.UUID;

import static com.comet.opik.api.evaluators.AutomationRuleEvaluatorTraceThreadLlmAsJudge.TraceThreadLlmAsJudgeCode;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * OPIK-8262, cross-layer half. The unit tests assert what the publisher is asked to write; this asserts what
 * actually lands on a real Redis stream and survives the shipped codec — one entry per thread id, each decodable
 * with its own id intact.
 *
 * <p>Covers the migration republish too: {@code OnlineScoringBaseScorer.migrateOrScoreThreadIds} rewrites a legacy
 * multi-id entry by handing the same {@code enqueueMessage} a list of single-id copies, which is the second case
 * below. It deliberately stops at the stream rather than driving the scorer end to end — the scorer's own branching
 * is unit-tested, and standing up the full scoring stack (ClickHouse traces, an LLM provider) to re-observe a Redis
 * write would test the harness more than the change.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class OnlineScorePublisherIntegrationTest {

    private final RedisContainer redis = RedisContainerUtils.newRedisContainer();
    private final PodamFactory podamFactory = PodamFactoryUtils.newPodamFactory();
    private final ServiceTogglesConfig serviceTogglesConfig = new ServiceTogglesConfig();

    @Mock
    private AutomationRuleEvaluatorService automationRuleEvaluatorService;

    private RedissonReactiveClient redissonClient;
    private OnlineScoringConfig config;
    private String streamName;

    @BeforeAll
    void setUpAll() {
        redis.start();
        var redissonConfig = new Config();
        redissonConfig.useSingleServer().setAddress(redis.getRedisURI()).setDatabase(0);
        redissonClient = Redisson.create(redissonConfig).reactive();

        streamName = "test-stream-%s".formatted(RandomStringUtils.secure().nextAlphanumeric(10).toLowerCase());
        config = OnlineScoringConfig.builder()
                .streamMaxLen(10_000)
                .streamTrimLimit(100)
                .streams(List.of(OnlineScoringConfig.StreamConfiguration.builder()
                        .scorer(AutomationRuleEvaluatorType.TRACE_THREAD_LLM_AS_JUDGE.getType())
                        .streamName(streamName)
                        .codec(RedisStreamCodec.JAVA.getName())
                        .build()))
                .build();
    }

    @AfterAll
    void tearDownAll() {
        if (redissonClient != null) {
            redissonClient.shutdown();
        }
        redis.stop();
    }

    @Test
    @DisplayName("A batch enqueue lands one decodable entry per thread id on the stream")
    void enqueueThreadMessageWritesOneEntryPerThreadId() {
        var publisher = newPublisher();
        var threadIds = List.of("thread-a-" + suffix(), "thread-b-" + suffix(), "thread-c-" + suffix());
        var rule = AutomationRuleEvaluatorTraceThreadLlmAsJudge.builder()
                .id(UUID.randomUUID())
                .name(podamFactory.manufacturePojo(String.class))
                .code(podamFactory.manufacturePojo(TraceThreadLlmAsJudgeCode.class))
                .build();

        publisher.enqueueThreadMessage(threadIds, rule, UUID.randomUUID(), "workspace", "user").block();

        var written = readStream();
        assertThat(written)
                .as("the subscriber acks per entry, so each thread id needs its own entry to be retried alone")
                .hasSize(threadIds.size());
        assertThat(written).allSatisfy(message -> assertThat(message.threadIds()).hasSize(1));
        assertThat(written.stream().map(message -> message.threadIds().getFirst()).toList())
                .containsExactlyInAnyOrderElementsOf(threadIds);
    }

    /**
     * The shape the migration shim produces: single-id copies of one legacy entry, handed to the same
     * {@code enqueueMessage} the scorer calls. Asserts they survive the codec with their ids and rule intact.
     */
    @Test
    @DisplayName("A migration republish lands each single-id copy intact")
    void migrationRepublishWritesEachSingleIdCopy() {
        var publisher = newPublisher();
        var legacy = podamFactory.manufacturePojo(TraceThreadToScoreLlmAsJudge.class).toBuilder()
                .threadIds(List.of("legacy-a-" + suffix(), "legacy-b-" + suffix()))
                .code(podamFactory.manufacturePojo(TraceThreadLlmAsJudgeCode.class))
                .build();
        var copies = legacy.threadIds().stream()
                .map(threadId -> legacy.toBuilder().threadIds(List.of(threadId)).build())
                .toList();

        publisher.enqueueMessage(copies, AutomationRuleEvaluatorType.TRACE_THREAD_LLM_AS_JUDGE).block();

        var written = readStream();
        assertThat(written).hasSize(2);
        assertThat(written).allSatisfy(message -> {
            assertThat(message.threadIds()).hasSize(1);
            assertThat(message.ruleId()).isEqualTo(legacy.ruleId());
            assertThat(message.workspaceId()).isEqualTo(legacy.workspaceId());
        });
        assertThat(written.stream().map(message -> message.threadIds().getFirst()).toList())
                .containsExactlyInAnyOrderElementsOf(legacy.threadIds());
    }

    private OnlineScorePublisher newPublisher() {
        redissonClient.getStream(streamName, RedisStreamCodec.JAVA.getCodec()).delete().block();
        return new OnlineScorePublisherImpl(config, serviceTogglesConfig, redissonClient,
                automationRuleEvaluatorService);
    }

    private List<TraceThreadToScoreLlmAsJudge> readStream() {
        RStreamReactive<String, TraceThreadToScoreLlmAsJudge> stream = redissonClient.getStream(streamName,
                RedisStreamCodec.JAVA.getCodec());
        return stream.range(StreamMessageId.MIN, StreamMessageId.MAX).block()
                .values().stream()
                .map(entry -> entry.get(OnlineScoringConfig.PAYLOAD_FIELD))
                .toList();
    }

    private static String suffix() {
        return RandomStringUtils.secure().nextAlphanumeric(16);
    }
}
