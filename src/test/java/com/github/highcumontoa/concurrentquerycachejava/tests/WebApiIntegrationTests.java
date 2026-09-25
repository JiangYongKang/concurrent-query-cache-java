package com.github.highcumontoa.concurrentquerycachejava.tests;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Web 层端到端验证：查询/命中、显式失效、状态变化、失败原因对应的 HTTP 状态码。
 *
 * <p>默认配置：ttl=5000、stale 开关关闭、并发=2、等待超时=1000、计算超时=2000、
 * 后端默认延迟 300ms。
 */
@SpringBootTest
@AutoConfigureMockMvc
class WebApiIntegrationTests {

    @Autowired
    private MockMvc mockMvc;

    @BeforeEach
    void reset() throws Exception {
        mockMvc.perform(post("/api/admin/reset")).andExpect(status().isOk());
        mockMvc.perform(post("/api/admin/backend/delay").param("millis", "50"))
                .andExpect(status().isOk());
    }

    @Test
    void firstComputeThenHitThenInvalidateRecompute() throws Exception {
        // 第一次：COMPUTED
        mockMvc.perform(get("/api/query").param("key", "web-k"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPUTED"))
                .andExpect(jsonPath("$.coalesced").value(false))
                .andExpect(jsonPath("$.degraded").value(false))
                .andExpect(jsonPath("$.data.version").value(1));

        // 第二次：HIT，底层调用次数仍为 1
        mockMvc.perform(get("/api/query").param("key", "web-k"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("HIT"));

        // 显式失效后：重新计算
        mockMvc.perform(post("/api/admin/invalidate").param("key", "web-k"))
                .andExpect(status().isOk());
        mockMvc.perform(get("/api/query").param("key", "web-k"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPUTED"));

        mockMvc.perform(get("/api/admin/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.backendInvocations").value(2));
    }

    @Test
    void backendFailureReturnsBadGatewayWithReasonWhenStaleDisabled() throws Exception {
        // 先种一个新鲜值，再打开故障；新鲜缓存仍会命中，因此先失效迫使重算
        mockMvc.perform(get("/api/query").param("key", "web-fail")).andExpect(status().isOk());
        mockMvc.perform(post("/api/admin/invalidate").param("key", "web-fail"))
                .andExpect(status().isOk());
        mockMvc.perform(post("/api/admin/backend/failing").param("value", "true"))
                .andExpect(status().isOk());

        mockMvc.perform(get("/api/query").param("key", "web-fail"))
                .andExpect(status().isBadGateway())
                .andExpect(jsonPath("$.status").value("FAILED"))
                .andExpect(jsonPath("$.reason").value("COMPUTE_ERROR"))
                .andExpect(jsonPath("$.degraded").value(false));
    }

    @Test
    void versionBumpInvalidatesAndServesNewValue() throws Exception {
        mockMvc.perform(get("/api/query").param("key", "web-ver"))
                .andExpect(jsonPath("$.data.version").value(1));
        mockMvc.perform(post("/api/admin/backend/bump-version").param("key", "web-ver"))
                .andExpect(status().isOk());
        mockMvc.perform(get("/api/query").param("key", "web-ver"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPUTED"))
                .andExpect(jsonPath("$.data.version").value(2));
    }
}
