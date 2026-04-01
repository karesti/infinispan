package org.infinispan.rest.resources;

import static java.util.Map.entry;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_PROTOSTREAM_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_EVENT_STREAM;
import static org.infinispan.rest.framework.Method.GET;
import static org.infinispan.rest.framework.Method.POST;
import static org.infinispan.rest.resources.MediaTypeUtils.negotiateMediaType;
import static org.infinispan.rest.resources.ResourceUtil.asJsonResponseFuture;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.health.ClusterHealth;
import org.infinispan.health.Health;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.manager.EmbeddedCacheManagerAdmin;
import org.infinispan.metadata.Metadata;
import org.infinispan.rest.EventStream;
import org.infinispan.rest.InvocationHelper;
import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.framework.ResourceHandler;
import org.infinispan.rest.framework.RestRequest;
import org.infinispan.rest.framework.RestResponse;
import org.infinispan.rest.framework.impl.Invocations;
import org.infinispan.rest.logging.Log;
import org.infinispan.rest.operations.CacheOperationsHelper;
import org.infinispan.rest.resources.mcp.McpArgument;
import org.infinispan.rest.resources.mcp.McpConstants;
import org.infinispan.rest.resources.mcp.McpInputSchema;
import org.infinispan.rest.resources.mcp.McpPrompt;
import org.infinispan.rest.resources.mcp.McpPromptMessage;
import org.infinispan.rest.resources.mcp.McpProperty;
import org.infinispan.rest.resources.mcp.McpResource;
import org.infinispan.rest.resources.mcp.McpResourceTemplate;
import org.infinispan.rest.resources.mcp.McpTool;
import org.infinispan.rest.resources.mcp.McpType;
import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.actions.SecurityActions;
import org.infinispan.server.core.query.ProtobufMetadataManager;
import org.infinispan.server.core.query.impl.RemoteQueryManager;
import org.infinispan.stats.Stats;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * @since 16.0
 */
public class McpServerResource implements ResourceHandler {
   public static final String CACHE_NAME = "cacheName";
   public static final String COUNTER_NAME = "counterName";
   private final InvocationHelper invocationHelper;
   private final Map<String, McpTool> TOOLS;
   private static final List<McpResource> RESOURCES = List.of(
         new McpResource(
               "infinispan+logs://server?lines=200",
               "server log",
               """
               PRIMARY SOURCE OF INFORMATION FOR SERVER STATUS/HEALTH.
               Returns info about server status, health, errors, warnings,
               exceptions, startup/shutdown events, and troubleshooting
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://audit?lines=200",
               "audit log",
               """
               PRIMARY SOURCE OF INFORMATION FOR SERVER STATUS/SECURITY.
               Returns infor about security events: authentication attempts,
               authorization failures, suspicious activity
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://rest-access?lines=200",
               "REST access log",
                """
               PRIMARY SOURCE OF INFORMATION FOR SERVER STATUS/WORKLOAD.
               Returns info about REST API usage patterns: request rates,
               errors (4xx/5xx), slow endpoints, client IPs. Useful also
               for security auditing, i.e. detecting DoS attacks or suspicious
               activity
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://gc?lines=200",
               "gc log",
               """
               PRIMARY SOURCE OF INFORMATION FOR VM STATUS/MEMORY.
               Returns info about JVM garbage collection events, heap usage,
               memory allocation, GC pauses, and overall memory pressure on
               the running JVM instance
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://hotrod-access?lines=200",
               "Hot Rod access log",
               """
               PRIMARY SOURCE OF INFORMATION FOR HOTROD PROTOCOL WORKLOAD.
               Returns info about Hot Rod client connections and requests:
               request rates, response times, errors, connection patterns,
               and client IPs
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://memcached-access?lines=200",
               "Memcached access log",
               """
               PRIMARY SOURCE OF INFORMATION FOR MEMCACHED PROTOCOL WORKLOAD.
               Returns info about Memcached client connections and requests:
               request rates, response times, errors, connection patterns,
               and client IPs
               """,
               "text/plain"
         ),
         new McpResource(
               "infinispan+logs://resp-access?lines=200",
               "RESP access log",
               """
               PRIMARY SOURCE OF INFORMATION FOR REDIS PROTOCOL WORKLOAD.
               Returns info about RESP (Redis) client connections and requests:
               request rates, response times, errors, connection patterns,
               and client IPs
               """,
               "text/plain"
         )
   );
   private static final List<McpResourceTemplate> RESOURCE_TEMPLATES = List.of(
         new McpResourceTemplate(
               "infinispan+cache://{cacheName}/{key}",
               "cache value",
               "Runtime data information: retrieves a value from a cache",
               null
         ),
         new McpResourceTemplate(
               "infinispan+counter://{countername}",
               "counter value",
               "Runtime data information: retrieves a value from a counter",
               null
         ),
         new McpResourceTemplate(
               "infinispan+logs://{logType}?lines={lines}",
               "server logs",
               """
               PRIMARY SOURCE OF INFORMATION FOR SERVER STATUS/HEALTH. Useful to retrieve different types of server logs. Primary source
               of information to monitor server status, server health, troubleshoot
               issues, and audit security-related events.""",
               "text/plain"
         )
   );

   private static final List<McpPrompt> PROMPTS = List.of(
         new McpPrompt(
               "find-documentation",
               """
                  Helps find relevant Infinispan documentation on the official website for a specific topic. Primary source
                  of information MUST BE https://infinispan.org/documentation
               """,
               "Find Infinispan Documentation",
               new McpArgument("topic", "What to search for in the documentation (e.g., 'cache configuration', 'Hot Rod client')", true),
               new McpArgument("context", "Additional context about your goal or use case (optional)", false)
         ),
         new McpPrompt(
               "find-documentation-guided",
               "Helps find relevant Infinispan documentation with topic suggestions for common areas",
               "Find Infinispan Documentation (Guided)",
               new McpArgument("topic", "What to search for in the documentation (e.g., 'cache configuration', 'Hot Rod client')", true),
               new McpArgument("context", "Additional context about your goal or use case (optional)", false)
         ),
         new McpPrompt(
               "configure-cache",
               "Guides users through cache configuration with XML/YAML templates and trade-off explanations",
               "Configure Cache",
               new McpArgument("cacheType", "Cache mode: local, replicated, distributed, or invalidation", true),
               new McpArgument("features", "Comma-separated list of features to include (e.g., 'persistence,indexing,transactions,cross-site')", false)
         ),
         new McpPrompt(
               "diagnose-issue",
               "Structured troubleshooting based on symptoms with diagnosis checklists",
               "Diagnose Issue",
               new McpArgument("symptom", "Description of the issue or symptom being experienced", true),
               new McpArgument("category", "Issue category: cluster, serialization, performance, persistence, transactions, or cross-site", false)
         ),
         new McpPrompt(
               "setup-client",
               "Help with Infinispan client setup including connection code, dependencies, and configuration",
               "Setup Client",
               new McpArgument("clientType", "Client type: hotrod, rest, or embedded", true),
               new McpArgument("framework", "Framework integration: spring, quarkus, or cdi", false)
         ),
         new McpPrompt(
               "setup-cross-site",
               "Cross-site replication guidance with JGroups relay and cache backup configuration",
               "Setup Cross-Site Replication",
               new McpArgument("topology", "Replication topology: active-active or active-passive", true),
               new McpArgument("sites", "Number of sites as a string (e.g., '2', '3')", false)
         )
   );

   public McpServerResource(InvocationHelper invocationHelper) {
      this.invocationHelper = invocationHelper;
      this.TOOLS = Map.ofEntries(
            entry(
                  "getCacheNames",
                  new McpTool(
                        "getCacheNames",
                        "Runtime data inventory: retrieves all the available cache names. For server status/health, use log resources instead.",
                        new McpInputSchema(McpType.OBJECT),
                        this::getCacheNames
                  )
            ),
            entry(
                  "createCache",
                  new McpTool(
                        "createCache",
                        "Runtime data modification: creates a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              )
                        ),
                        this::createCache
                  )
            ),
            entry(
                  "getCacheEntry",
                  new McpTool(
                        "getCacheEntry",
                        "Runtime data retrieval: retrieves a value from a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "key",
                                    McpType.STRING,
                                    "The key of the entry",
                                    true
                              )
                        ),
                        this::getCacheValue
                  )
            ),
            entry(
                  "setCacheEntry",
                  new McpTool(
                        "setCacheEntry",
                        "Runtime data modification: inserts/updates an entry in a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "key",
                                    McpType.STRING,
                                    "The key of the entry",
                                    true
                              ),
                              new McpProperty(
                                    "value",
                                    McpType.STRING,
                                    "The value of the entry",
                                    true
                              ),
                              new McpProperty(
                                    "lifespan",
                                    McpType.NUMBER,
                                    "The lifespan of the entry in milliseconds",
                                    false
                              ),
                              new McpProperty(
                                    "maxIdle",
                                    McpType.NUMBER,
                                    "The maximum idle time of the entry in milliseconds",
                                    false
                              )
                        ),
                        this::setCacheValue
                  )
            ),
            entry(
                  "deleteCacheEntry",
                  new McpTool(
                        "deleteCacheEntry",
                        "Runtime data modification: deletes an entry from a cache",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "key",
                                    McpType.STRING,
                                    "The key of the entry",
                                    true
                              )
                        ),
                        this::deleteCacheValue
                  )
            ),
            entry(
                  "queryCache",
                  new McpTool(
                        "queryCache",
                        "Runtime data retrieval: queries a cache using Ickle query language",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              ),
                              new McpProperty(
                                    "query",
                                    McpType.STRING,
                                    "The Ickle query",
                                    true
                              )
                        ),
                        this::queryCache
                  )
            ),
            entry(
                  "getSchemas",
                  new McpTool(
                        "getSchemas",
                        "Runtime data inventory: retrieves all the available schemas. For server status/health, use log resources instead.",
                        new McpInputSchema(McpType.OBJECT),
                        this::getSchemas
                  )
            ),
            entry(
                  "getCounterNames",
                  new McpTool(
                        "getCounterNames",
                        "Runtime data inventory: retrieves all the available counter names. For server status/health, use log resources instead.",
                        new McpInputSchema(McpType.OBJECT),
                        this::getCounterNames
                  )
            ),
            entry(
                  "getCounter",
                  new McpTool(
                        "getCounter",
                        "Runtime data retrieval: retrieves the value of a counter",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    COUNTER_NAME,
                                    McpType.STRING,
                                    "The name of the counter",
                                    true
                              )
                        ),
                        this::getCounter
                  )
            ),
            entry(
                  "increment",
                  new McpTool(
                        "increment",
                        "Runtime data modification: increments the value of a counter",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    COUNTER_NAME,
                                    McpType.STRING,
                                    "The name of the counter",
                                    true
                              )
                        ),
                        this::incrementCounter
                  )
            ),
            entry(
                  "decrement",
                  new McpTool(
                        "decrement",
                        "Runtime data modification: decrements the value of a counter",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    COUNTER_NAME,
                                    McpType.STRING,
                                    "The name of the counter",
                                    true
                              )
                        ),
                        this::decrementCounter
                  )
            ),
            entry(
                  "getCacheConfiguration",
                  new McpTool(
                        "getCacheConfiguration",
                        "Runtime data retrieval: retrieves a cache's configuration as JSON",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              )
                        ),
                        this::getCacheConfiguration
                  )
            ),
            entry(
                  "getClusterHealth",
                  new McpTool(
                        "getClusterHealth",
                        "Runtime data retrieval: returns cluster health information including status, cluster name, node count, and member names",
                        new McpInputSchema(McpType.OBJECT),
                        this::getClusterHealth
                  )
            ),
            entry(
                  "getCacheStats",
                  new McpTool(
                        "getCacheStats",
                        "Runtime data retrieval: returns cache statistics including entries count, hits, misses, stores, evictions, and average read/write times",
                        new McpInputSchema(
                              McpType.OBJECT,
                              new McpProperty(
                                    CACHE_NAME,
                                    McpType.STRING,
                                    "The name of the cache",
                                    true
                              )
                        ),
                        this::getCacheStats
                  )
            )
      );
   }

   @Override
   public Invocations getInvocations() {
      return new Invocations.Builder("mcp", "Model Context Protocol")
            .invocation().methods(GET, POST).path("/v3/mcp")
            .handleWith(this::mcp)
            .create();
   }

   private CompletionStage<Json> getSchemas(RestRequest request, Json json) {
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(ProtobufMetadataManager.PROTOBUF_METADATA_CACHE_NAME, request);
      return CompletableFuture.supplyAsync(() -> {
         Json schemas = Json.array();
         for (Map.Entry<Object, Object> entry : cache.entrySet()) {
            schemas.add(Json.object().set("name", entry.getKey()).set("schema", entry.getValue()));
         }
         return schemas;
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> mcp(RestRequest request) {
      String content = request.contents().asString();
      MediaType contentType = request.contentType();
      if (MediaType.APPLICATION_JSON.equals(contentType)) {
         // Single call
         Json json = Json.read(content);
         if (!json.isObject()) {
            throw Log.REST.invalidContent();
         }
         if (!"2.0".equals(json.at(McpConstants.JSONRPC).asString())) {
            throw Log.REST.invalidContent();
         }
         String method = json.at("method").asString();
         return switch (method) {
            // These should be the most frequently invoked methods
            case "resources/read" -> mcpResourcesRead(request, json);
            case "tools/call" -> mcpToolsCall(request, json);
            // All other calls
            case "completion/complete" -> mcpCompletionComplete(request, json);
            case "initialize" -> mcpInitialize(request, json);
            // case "logging/setLevel" -> mcpLoggingSetLevel(request, json);
            case "prompts/get" -> mcpPromptsGet(request, json);
            case "prompts/list" -> mcpPromptsList(request, json);
            case "resources/list" -> mcpResourcesList(request, json);
            case "resources/subscribe" -> mcpResourcesSubscribe(request, json);
            case "resources/templates/list" -> mcpResourcesTemplatesList(request, json);
            case "resources/unsubscribe" -> mcpResourcesUnubscribe(request, json);
            case "notifications/initialized" -> mcpNotificationsInitialized(request, json);
            case "tools/list" -> mcpToolsList(request, json);
            default -> throw Log.REST.invalidContent();
         };
      } else {
         // SSE
         NettyRestResponse.Builder responseBuilder = invocationHelper.newResponse(request);
         responseBuilder.contentType(TEXT_EVENT_STREAM).entity(newEventStream());
         return CompletableFuture.completedFuture(responseBuilder.build());
      }
   }

   private EventStream newEventStream() {
      /*
       * Messages that can be returned here
       * "notifications/message"
       * "notifications/prompts/list_changed"
       * "notifications/resources/list_changed"
       * "notifications/resources/updated"
       * "notifications/tools/list_changed"
       */
      return new EventStream(null, () -> {
      });
   }

   private CompletionStage<RestResponse> mcpPromptsGet(RestRequest request, Json json) {
      Json params = json.at(McpConstants.PARAMS);
      String name = params.at("name").asString();
      Json arguments = params.at("arguments");

      // Find the matching prompt
      McpPrompt prompt = PROMPTS.stream()
            .filter(p -> p.name().equals(name))
            .findFirst()
            .orElse(null);

      if (prompt == null) {
         Json response = rpcResponse(json)
               .set("error", Json.object()
                     .set("code", McpConstants.METHOD_NOT_FOUND)
                     .set("message", "Prompt not found: " + name));
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      McpPromptMessage message;

      switch (name) {
         case "find-documentation", "find-documentation-guided" -> {
            String topic = arguments.has("topic") ? arguments.at("topic").asString() : null;
            String context = arguments.has("context") ? arguments.at("context").asString() : null;
            if (topic == null || topic.isEmpty()) {
               Json response = rpcResponse(json)
                     .set("error", Json.object()
                           .set("code", McpConstants.INVALID_PARAMS)
                           .set("message", "Required argument 'topic' is missing"));
               return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
            }
            message = buildDocumentationPromptMessage(topic, context);
         }
         case "configure-cache" -> {
            String cacheType = arguments.has("cacheType") ? arguments.at("cacheType").asString() : null;
            String features = arguments.has("features") ? arguments.at("features").asString() : null;
            if (cacheType == null || cacheType.isEmpty()) {
               Json response = rpcResponse(json)
                     .set("error", Json.object()
                           .set("code", McpConstants.INVALID_PARAMS)
                           .set("message", "Required argument 'cacheType' is missing"));
               return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
            }
            message = buildConfigureCachePromptMessage(cacheType, features);
         }
         case "diagnose-issue" -> {
            String symptom = arguments.has("symptom") ? arguments.at("symptom").asString() : null;
            String category = arguments.has("category") ? arguments.at("category").asString() : null;
            if (symptom == null || symptom.isEmpty()) {
               Json response = rpcResponse(json)
                     .set("error", Json.object()
                           .set("code", McpConstants.INVALID_PARAMS)
                           .set("message", "Required argument 'symptom' is missing"));
               return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
            }
            message = buildDiagnoseIssuePromptMessage(symptom, category);
         }
         case "setup-client" -> {
            String clientType = arguments.has("clientType") ? arguments.at("clientType").asString() : null;
            String framework = arguments.has("framework") ? arguments.at("framework").asString() : null;
            if (clientType == null || clientType.isEmpty()) {
               Json response = rpcResponse(json)
                     .set("error", Json.object()
                           .set("code", McpConstants.INVALID_PARAMS)
                           .set("message", "Required argument 'clientType' is missing"));
               return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
            }
            message = buildSetupClientPromptMessage(clientType, framework);
         }
         case "setup-cross-site" -> {
            String topology = arguments.has("topology") ? arguments.at("topology").asString() : null;
            String sites = arguments.has("sites") ? arguments.at("sites").asString() : null;
            if (topology == null || topology.isEmpty()) {
               Json response = rpcResponse(json)
                     .set("error", Json.object()
                           .set("code", McpConstants.INVALID_PARAMS)
                           .set("message", "Required argument 'topology' is missing"));
               return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
            }
            message = buildSetupCrossSitePromptMessage(topology, sites);
         }
         default -> {
            Json response = rpcResponse(json)
                  .set("error", Json.object()
                        .set("code", McpConstants.METHOD_NOT_FOUND)
                        .set("message", "Unknown prompt: " + name));
            return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
         }
      }

      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("messages", Json.array()
                        .add(message.toJson())
                  )
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpPromptsList(RestRequest request, Json json) {
      Json prompts = Json.array();
      for (McpPrompt prompt : PROMPTS) {
         prompts.add(prompt.toJson());
      }
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("prompts", prompts)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpCompletionComplete(RestRequest request, Json json) {
      Json params = json.at(McpConstants.PARAMS);
      Json argument = params.at("argument");
      String argumentName = argument.at("name").asString();
      Json ref = params.at("ref");
      String refType = ref.at("type").asString();

      // Only support completion for prompts
      if (!"ref/prompt".equals(refType)) {
         Json response = rpcResponse(json)
               .set("error", Json.object()
                     .set("code", McpConstants.METHOD_NOT_FOUND)
                     .set("message", "Completion not supported for: " + refType));
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // Extract prompt name from URI (e.g., "prompt://find-documentation-guided")
      String refName = ref.at("name").asString();

      // Only provide completions for the guided prompt and topic argument
      if ("find-documentation-guided".equals(refName) && "topic".equals(argumentName)) {
         Json completions = Json.array()
               .add(Json.object().set("value", "cache configuration").set("label", "Cache Configuration"))
               .add(Json.object().set("value", "cross-site replication").set("label", "Cross-Site Replication"))
               .add(Json.object().set("value", "Hot Rod client").set("label", "Hot Rod Client"))
               .add(Json.object().set("value", "query API").set("label", "Query API"))
               .add(Json.object().set("value", "REST API").set("label", "REST API"))
               .add(Json.object().set("value", "persistence").set("label", "Persistence"))
               .add(Json.object().set("value", "security").set("label", "Security"))
               .add(Json.object().set("value", "clustering").set("label", "Clustering"))
               .add(Json.object().set("value", "transactions").set("label", "Transactions"))
               .add(Json.object().set("value", "server management").set("label", "Server Management"));

         Json response = rpcResponse(json)
               .set(McpConstants.RESULT, Json.object()
                     .set("completion", Json.object()
                           .set("values", completions)
                           .set("total", completions.asJsonList().size())
                           .set("hasMore", false)
                     )
               );
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // Completions for configure-cache cacheType argument
      if ("configure-cache".equals(refName) && "cacheType".equals(argumentName)) {
         Json completions = Json.array()
               .add(Json.object().set("value", "local").set("label", "Local"))
               .add(Json.object().set("value", "replicated").set("label", "Replicated"))
               .add(Json.object().set("value", "distributed").set("label", "Distributed"))
               .add(Json.object().set("value", "invalidation").set("label", "Invalidation"));

         Json response = rpcResponse(json)
               .set(McpConstants.RESULT, Json.object()
                     .set("completion", Json.object()
                           .set("values", completions)
                           .set("total", completions.asJsonList().size())
                           .set("hasMore", false)
                     )
               );
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // Completions for diagnose-issue category argument
      if ("diagnose-issue".equals(refName) && "category".equals(argumentName)) {
         Json completions = Json.array()
               .add(Json.object().set("value", "cluster").set("label", "Cluster"))
               .add(Json.object().set("value", "serialization").set("label", "Serialization"))
               .add(Json.object().set("value", "performance").set("label", "Performance"))
               .add(Json.object().set("value", "persistence").set("label", "Persistence"))
               .add(Json.object().set("value", "transactions").set("label", "Transactions"))
               .add(Json.object().set("value", "cross-site").set("label", "Cross-Site"));

         Json response = rpcResponse(json)
               .set(McpConstants.RESULT, Json.object()
                     .set("completion", Json.object()
                           .set("values", completions)
                           .set("total", completions.asJsonList().size())
                           .set("hasMore", false)
                     )
               );
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // Completions for setup-client clientType argument
      if ("setup-client".equals(refName) && "clientType".equals(argumentName)) {
         Json completions = Json.array()
               .add(Json.object().set("value", "hotrod").set("label", "Hot Rod"))
               .add(Json.object().set("value", "rest").set("label", "REST"))
               .add(Json.object().set("value", "embedded").set("label", "Embedded"));

         Json response = rpcResponse(json)
               .set(McpConstants.RESULT, Json.object()
                     .set("completion", Json.object()
                           .set("values", completions)
                           .set("total", completions.asJsonList().size())
                           .set("hasMore", false)
                     )
               );
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // Completions for setup-client framework argument
      if ("setup-client".equals(refName) && "framework".equals(argumentName)) {
         Json completions = Json.array()
               .add(Json.object().set("value", "spring").set("label", "Spring"))
               .add(Json.object().set("value", "quarkus").set("label", "Quarkus"))
               .add(Json.object().set("value", "cdi").set("label", "CDI"));

         Json response = rpcResponse(json)
               .set(McpConstants.RESULT, Json.object()
                     .set("completion", Json.object()
                           .set("values", completions)
                           .set("total", completions.asJsonList().size())
                           .set("hasMore", false)
                     )
               );
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // Completions for setup-cross-site topology argument
      if ("setup-cross-site".equals(refName) && "topology".equals(argumentName)) {
         Json completions = Json.array()
               .add(Json.object().set("value", "active-active").set("label", "Active-Active"))
               .add(Json.object().set("value", "active-passive").set("label", "Active-Passive"));

         Json response = rpcResponse(json)
               .set(McpConstants.RESULT, Json.object()
                     .set("completion", Json.object()
                           .set("values", completions)
                           .set("total", completions.asJsonList().size())
                           .set("hasMore", false)
                     )
               );
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      // No completions for other prompts or arguments
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("completion", Json.object()
                        .set("values", Json.array())
                        .set("total", 0)
                        .set("hasMore", false)
                  )
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpInitialize(RestRequest request, Json json) {
      Json params = json.at("params");
      String protocolVersion = params.has("protocolVersion") ? params.at("protocolVersion").asString() : McpConstants.MCP_VERSION;
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("capabilities", Json.object()
                              .set("resources", Json.object()
                                    .set("subscribe", false) // whether the client can subscribe to be notified of changes to individual resources.
                                    .set("listChanged", false) // whether the server will emit notifications when the list of available resources changes.
                              )
                              // .set("logging", Json.object()) // Enable when we support logging
                              .set("tools", Json.object()
                                    .set("listChanged", false) //indicates whether the server will emit notifications when the list of available tools changes.
                              )
                              .set("prompts", Json.object()
                                    .set("listChanged", false) // whether the server will emit notifications when the list of available prompts changes.
                              )
                  )
                  .set("serverInfo", Json.object()
                        .set("version", Version.getVersion())
                        .set("name", Version.getBrandName())
                  )
                  .set("protocolVersion", protocolVersion)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request).header(McpConstants.MCP_SESSION_ID, UUID.randomUUID().toString()), response, false);
   }

   private static Json rpcResponse(Json request) {
      Json json = Json.object()
            .set(McpConstants.JSONRPC, "2.0");
      if (request.has("id")) {
         json.set("id", request.at("id").asInteger());
      }
      return json;
   }

   private CompletionStage<RestResponse> mcpResourcesList(RestRequest request, Json json) {
      Json resources = Json.array();
      for (McpResource resource : RESOURCES) {
         resources.add(resource.toJson());
      }
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("resources", resources)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpResourcesTemplatesList(RestRequest request, Json json) {
      Json resourceTemplates = Json.array();
      for (McpResourceTemplate template : RESOURCE_TEMPLATES) {
         resourceTemplates.add(template.toJson());
      }
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("resourceTemplates", resourceTemplates)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpResourcesRead(RestRequest request, Json json) {
      Json params = json.at(McpConstants.PARAMS);
      String uri = params.at("uri").asString();

      // Check if this is a log resource request
      if (uri != null && uri.startsWith("infinispan+logs://")) {
         return handleLogResourceRead(request, json, uri);
      }

      // For other resource types, return not supported
      return unimplemented(request, json, "Resource type not supported: " + uri);
   }

   private CompletionStage<RestResponse> handleLogResourceRead(RestRequest request, Json json, String uri) {
      // Check ADMIN permission for log access
      try {
         invocationHelper.getRestCacheManager().getAuthorizer()
               .checkPermission(AuthorizationPermission.ADMIN);
      } catch (SecurityException e) {
         Json response = rpcResponse(json)
               .set("error", Json.object()
                     .set("code", McpConstants.FORBIDDEN)
                     .set("message", "Admin permission required to access log resources"));
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }

      return CompletableFuture.supplyAsync(() -> {
         try {
            // Parse URI: infinispan+logs://server?lines=200
            java.net.URI parsedUri = java.net.URI.create(uri);
            String logType = parsedUri.getHost();
            Map<String, String> queryParams = parseQueryParameters(parsedUri.getQuery());

            // Get lines parameter (default: 200, max: 10000)
            int lines = 200;
            if (queryParams.containsKey("lines")) {
               try {
                  lines = Integer.parseInt(queryParams.get("lines"));
                  if (lines < 1) {
                     lines = 1;
                  } else if (lines > 10000) {
                     lines = 10000;
                  }
               } catch (NumberFormatException e) {
                  throw new IllegalArgumentException("Invalid lines parameter: " + queryParams.get("lines"));
               }
            }

            // Get log file path
            String logPath = System.getProperty("infinispan.server.log.path");
            if (logPath == null) {
               throw new IllegalStateException("Log path not configured (infinispan.server.log.path property not set)");
            }

            String fileName = getLogFileName(logType);
            java.nio.file.Path logFile = java.nio.file.Paths.get(logPath, fileName);

            // Read last N lines
            String content = readLastLines(logFile, lines);

            // Build MCP response
            Json response = rpcResponse(json)
                  .set(McpConstants.RESULT, Json.object()
                        .set("contents", Json.array()
                              .add(Json.object()
                                    .set("uri", uri)
                                    .set("mimeType", "text/plain")
                                    .set("text", content)
                              )
                        )
                  );

            NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
            return builder.entity(response.toString())
                  .contentType(MediaType.APPLICATION_JSON)
                  .status(HttpResponseStatus.OK)
                  .build();

         } catch (IllegalArgumentException e) {
            // Invalid log type or parameters
            Json response = rpcResponse(json)
                  .set("error", Json.object()
                        .set("code", McpConstants.INVALID_PARAMS)
                        .set("message", e.getMessage()));
            NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
            return builder.entity(response.toString())
                  .contentType(MediaType.APPLICATION_JSON)
                  .status(HttpResponseStatus.BAD_REQUEST)
                  .build();
         } catch (java.io.IOException e) {
            // File read error
            Json response = rpcResponse(json)
                  .set("error", Json.object()
                        .set("code", McpConstants.INTERNAL_ERROR)
                        .set("message", "Error reading log file: " + e.getMessage()));
            NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
            return builder.entity(response.toString())
                  .contentType(MediaType.APPLICATION_JSON)
                  .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                  .build();
         } catch (Exception e) {
            // Unexpected error
            Json response = rpcResponse(json)
                  .set("error", Json.object()
                        .set("code", McpConstants.INTERNAL_ERROR)
                        .set("message", "Unexpected error: " + e.getMessage()));
            NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
            return builder.entity(response.toString())
                  .contentType(MediaType.APPLICATION_JSON)
                  .status(HttpResponseStatus.INTERNAL_SERVER_ERROR)
                  .build();
         }
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<RestResponse> mcpResourcesUnubscribe(RestRequest request, Json json) {
      return unimplemented(request, json, "resources/unsubscribe not supported");
   }

   private CompletionStage<RestResponse> mcpResourcesSubscribe(RestRequest request, Json json) {
      return unimplemented(request, json, "resources/subscribe not supported");
   }

   private CompletionStage<RestResponse> mcpNotificationsInitialized(RestRequest request, Json json) {
      return unimplemented(request, json, "notifications/initialized supported");
   }

   private CompletionStage<RestResponse> unimplemented(RestRequest request, Json json, String message) {
      Json response = rpcResponse(json)
            .set("error", Json.object().set("code", McpConstants.METHOD_NOT_FOUND).set("message", message));
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpToolsList(RestRequest request, Json json) {
      Json tools = Json.array();
      for (McpTool tool : TOOLS.values()) {
         tools.add(tool.toJson());
      }
      Json response = rpcResponse(json)
            .set(McpConstants.RESULT, Json.object()
                  .set("tools", tools)
            );
      return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
   }

   private CompletionStage<RestResponse> mcpToolsCall(RestRequest request, Json json) {
      Json params = json.at(McpConstants.PARAMS);
      String name = params.at(McpConstants.NAME).asString();
      McpTool mcpTool = TOOLS.get(name);
      if (mcpTool == null) {
         Json response = rpcResponse(json)
               .set("error", Json.object().set("code", McpConstants.METHOD_NOT_FOUND).set("message", "Tool not found: " + name));
         return asJsonResponseFuture(invocationHelper.newResponse(request), response, false);
      }
      Json arguments = params.at("arguments");
      Json response = rpcResponse(json);
      return mcpTool.callback().apply(request, arguments).handle((j, t) -> {
         response.set(McpConstants.RESULT, Json.object()
               .set("isError", t != null)
               .set("content", j)
         );
         NettyRestResponse.Builder builder = invocationHelper.newResponse(request);
         return builder.entity(response.toString()).contentType(MediaType.APPLICATION_JSON).status(HttpResponseStatus.OK).build();
      });
   }

   private CompletionStage<Json> getCacheNames(RestRequest request, Json args) {
      Collection<String> cacheNames = invocationHelper.getRestCacheManager().getAccessibleCacheNames();
      return CompletableFuture.completedFuture(Json.array(textResult(Json.make(cacheNames).toString())));
   }

   private CompletionStage<Json> createCache(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      EmbeddedCacheManagerAdmin admin = invocationHelper.getRestCacheManager().getCacheManagerAdmin(request);
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC).encoding().mediaType(APPLICATION_PROTOSTREAM_TYPE);
      return CompletableFuture.supplyAsync(() -> {
         admin.createCache(cacheName, builder.build());
         return Json.array();
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> getCacheValue(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      String key = args.at("key").asString();
      AdvancedCache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      MediaType requestedMediaType = negotiateMediaType(cache, invocationHelper.getEncoderRegistry(), request);
      return invocationHelper.getRestCacheManager()
            .getInternalEntry(cacheName, key, MediaType.TEXT_PLAIN, requestedMediaType, request)
            .thenApply(entry -> Json.array(textResult(entry.getValue().toString())));
   }

   private static Json textResult(String value) {
      return Json.object().set("text", value).set("type", "text");
   }

   private CompletionStage<Json> setCacheValue(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      String key = args.at("key").asString();
      String value = args.at("value").asString();
      Long lifespan = args.has("lifespan") ? args.at("lifespan").asLong() : null;
      Long maxidle = args.has("maxidle") ? args.at("maxidle").asLong() : null;
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, MediaType.TEXT_PLAIN, MediaType.TEXT_PLAIN, request);
      Configuration config = SecurityActions.getCacheConfiguration(cache);
      final Metadata metadata = CacheOperationsHelper.createMetadata(config, lifespan, maxidle);
      return cache.putAsync(key, value, metadata).thenApply(__ -> Json.array());
   }

   private CompletionStage<Json> deleteCacheValue(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      String key = args.at("key").asString();
      return invocationHelper.getRestCacheManager()
            .remove(cacheName, key, MediaType.TEXT_PLAIN, request)
            .thenApply(entry -> Json.array());
   }

   private CompletionStage<Json> queryCache(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      String query = args.at("query").asString();
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, request);
      RemoteQueryManager remoteQueryManager = SecurityActions.getCacheComponentRegistry(cache).getComponent(RemoteQueryManager.class);
      return CompletableFuture.supplyAsync(() -> {
         try {
            byte[] queryResultBytes = remoteQueryManager.executeQuery(query, Map.of(), 0, 100, 10000, cache, MediaType.APPLICATION_JSON, false);
            String result = new String(queryResultBytes, java.nio.charset.StandardCharsets.UTF_8);
            return Json.array(textResult(result));
         } catch (Exception e) {
            return Json.array(textResult("Error executing query: " + e.getMessage()));
         }
      }, invocationHelper.getExecutor());
   }

   private CompletionStage<Json> getCounterNames(RestRequest request, Json args) {
      Collection<String> counterNames = invocationHelper.getCounterManager().getCounterNames();
      return CompletableFuture.completedFuture(Json.array(textResult(Json.make(counterNames).toString())));
   }

   private CompletionStage<Json> getCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      return invocationHelper.getCounterManager().getWeakCounterAsync(counterName)
            .thenApply(WeakCounter::getValue)
            .thenApply(v -> Json.array(textResult(v.toString())));
   }

   private CompletionStage<Json> incrementCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      return invocationHelper.getCounterManager().getWeakCounterAsync(counterName)
            .thenApply(WeakCounter::increment)
            .thenApply(__ -> Json.array());
   }

   private CompletionStage<Json> decrementCounter(RestRequest request, Json args) {
      String counterName = args.at(COUNTER_NAME).asString();
      return invocationHelper.getCounterManager().getWeakCounterAsync(counterName)
            .thenApply(WeakCounter::decrement)
            .thenApply(__ -> Json.array());
   }

   private CompletionStage<Json> getCacheConfiguration(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      AdvancedCache<Object, Object> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      Configuration configuration = SecurityActions.getCacheConfiguration(cache);
      String configString = configuration.toStringConfiguration(cacheName, MediaType.APPLICATION_JSON, false);
      return CompletableFuture.completedFuture(Json.array(textResult(configString)));
   }

   private CompletionStage<Json> getClusterHealth(RestRequest request, Json args) {
      EmbeddedCacheManager cacheManager = invocationHelper.getRestCacheManager().getInstance();
      Health health = cacheManager.withSubject(request.getSubject()).getHealth();
      ClusterHealth clusterHealth = health.getClusterHealth();
      Json result = Json.object()
            .set("status", clusterHealth.getHealthStatus().toString())
            .set("cluster_name", clusterHealth.getClusterName())
            .set("number_of_nodes", clusterHealth.getNumberOfNodes())
            .set("node_names", Json.make(clusterHealth.getNodeNames()));
      return CompletableFuture.completedFuture(Json.array(textResult(result.toString())));
   }

   private CompletionStage<Json> getCacheStats(RestRequest request, Json args) {
      String cacheName = args.at(CACHE_NAME).asString();
      Cache<?, ?> cache = invocationHelper.getRestCacheManager().getCache(cacheName, request);
      return CompletableFuture.supplyAsync(() -> {
         Stats stats = cache.getAdvancedCache().getStats();
         Json result = Json.object()
               .set("approximate_entries", stats.getApproximateEntries())
               .set("approximate_entries_in_memory", stats.getApproximateEntriesInMemory())
               .set("hits", stats.getHits())
               .set("misses", stats.getMisses())
               .set("stores", stats.getStores())
               .set("evictions", stats.getEvictions())
               .set("average_read_time", stats.getAverageReadTime())
               .set("average_read_time_nanos", stats.getAverageReadTimeNanos())
               .set("average_write_time_nanos", stats.getAverageWriteTimeNanos())
               .set("average_remove_time_nanos", stats.getAverageRemoveTimeNanos())
               .set("time_since_start", stats.getTimeSinceStart())
               .set("time_since_reset", stats.getTimeSinceReset());
         return Json.array(textResult(result.toString()));
      }, invocationHelper.getExecutor());
   }

   /**
    * Builds a prompt message for cache configuration guidance.
    */
   private McpPromptMessage buildConfigureCachePromptMessage(String cacheType, String features) {
      StringBuilder messageText = new StringBuilder();
      messageText.append("Help me configure an Infinispan cache with the following requirements:\n\n");
      messageText.append("Cache mode: ").append(cacheType).append("\n\n");

      messageText.append("Please explain the trade-offs of the '").append(cacheType).append("' cache mode:\n");
      switch (cacheType) {
         case "local" -> messageText.append("""
               - Local caches do not join a cluster and do not share data with other nodes.
               - They provide the best performance since there is no network overhead.
               - However, data is not replicated, so there is no fault tolerance.
               - Best for: single-node deployments, temporary/non-critical data, or as L1 caches.
               """);
         case "replicated" -> messageText.append("""
               - Replicated caches copy all data to every node in the cluster.
               - Reads are fast since data is always local, but writes are slower as they must propagate to all nodes.
               - Memory usage scales linearly with data size (every node stores everything).
               - Best for: small datasets that are read-frequently, configuration data, or lookup tables.
               """);
         case "distributed" -> messageText.append("""
               - Distributed caches store data on a subset of nodes (configurable via numOwners, default 2).
               - Provides a good balance of performance, scalability, and fault tolerance.
               - Reads may require network hops if the data is not on the local node.
               - Best for: large datasets, horizontally scalable architectures.
               """);
         case "invalidation" -> messageText.append("""
               - Invalidation caches do not share data; instead, they remove stale entries on other nodes.
               - Typically used with a shared external data store (e.g., a database).
               - When an entry is modified, other nodes are notified to evict their local copy.
               - Best for: caching data from an external store to reduce DB load.
               """);
         default -> messageText.append("Unknown cache type. Valid values: local, replicated, distributed, invalidation.\n");
      }

      if (features != null && !features.isEmpty()) {
         messageText.append("\nRequested features: ").append(features).append("\n");
         messageText.append("Please include configuration for these features in the XML and YAML templates.\n");
      }

      messageText.append("""

            Please provide:
            1. An XML configuration template for this cache mode
            2. A YAML configuration template for this cache mode
            3. A programmatic (Java) configuration example
            4. Important configuration parameters and their recommended values
            5. Any caveats or best practices for this cache mode
            """);

      return new McpPromptMessage("user", messageText.toString());
   }

   /**
    * Builds a prompt message for issue diagnosis.
    */
   private McpPromptMessage buildDiagnoseIssuePromptMessage(String symptom, String category) {
      StringBuilder messageText = new StringBuilder();
      messageText.append("I need help diagnosing an Infinispan issue.\n\n");
      messageText.append("Symptom: ").append(symptom).append("\n");

      if (category != null && !category.isEmpty()) {
         messageText.append("Category: ").append(category).append("\n\n");
         messageText.append("Diagnosis checklist for '").append(category).append("' issues:\n");
         switch (category) {
            case "cluster" -> messageText.append("""
                  1. Check cluster health using the getClusterHealth tool
                  2. Verify JGroups configuration and network connectivity between nodes
                  3. Check for split-brain scenarios or partitioned clusters
                  4. Review server logs for WARN/ERROR messages related to clustering
                  5. Verify that all nodes are running the same Infinispan version
                  6. Check firewall rules and port accessibility (default: 7800 for JGroups, 11222 for Hot Rod)
                  7. Look for GC pauses that might cause false failure detection
                  """);
            case "serialization" -> messageText.append("""
                  1. Verify that all entities are properly annotated with @ProtoField/@ProtoFactory
                  2. Check that Protobuf schemas are registered in the server
                  3. Ensure client and server marshallers are compatible
                  4. Verify MediaType configuration matches the data format
                  5. Check for ClassNotFoundException or marshalling errors in logs
                  6. Ensure all custom types have proper serialization context initializers
                  """);
            case "performance" -> messageText.append("""
                  1. Check cache statistics using the getCacheStats tool
                  2. Review hit/miss ratios to evaluate cache effectiveness
                  3. Check for excessive evictions indicating memory pressure
                  4. Review GC logs for long pauses or high allocation rates
                  5. Verify cache encoding and whether conversions are occurring on reads/writes
                  6. Check if indexing is enabled and whether queries are using indexes
                  7. Review number of owners and cache mode for distributed caches
                  8. Check for hot keys causing contention
                  """);
            case "persistence" -> messageText.append("""
                  1. Verify persistence store configuration and connectivity
                  2. Check for errors in server logs related to cache stores
                  3. Verify file permissions for file-based stores
                  4. Check database connectivity and schema for JDBC stores
                  5. Review passivation and preload settings
                  6. Check for slow store operations affecting cache performance
                  7. Verify that shared vs. non-shared store configuration is correct for the cluster
                  """);
            case "transactions" -> messageText.append("""
                  1. Verify transaction manager configuration
                  2. Check for deadlocks or lock timeout errors in logs
                  3. Review transaction mode (FULL_XA, NON_XA, NON_DURABLE_XA, BATCH)
                  4. Check lock acquisition timeout settings
                  5. Verify isolation level configuration (READ_COMMITTED, REPEATABLE_READ)
                  6. Look for TransactionXaException or RollbackException in logs
                  7. Check if transaction recovery is properly configured
                  """);
            case "cross-site" -> messageText.append("""
                  1. Verify JGroups RELAY2 configuration on all sites
                  2. Check network connectivity between sites
                  3. Review backup strategy (sync vs async) and timeout settings
                  4. Check for backup failure policies and state transfer status
                  5. Verify site names match between configuration and actual deployment
                  6. Look for cross-site related warnings or errors in logs
                  7. Check if sites are online/offline using the REST API
                  """);
            default -> messageText.append("Unknown category. Valid values: cluster, serialization, performance, persistence, transactions, cross-site.\n");
         }
      } else {
         messageText.append("""

               General diagnosis checklist:
               1. Check cluster health using the getClusterHealth tool
               2. Check cache statistics using the getCacheStats tool
               3. Review server logs for WARN/ERROR messages
               4. Check GC logs for memory pressure
               5. Verify configuration is correct for the deployment scenario
               """);
      }

      messageText.append("""

            Please analyze the symptom and provide:
            1. Most likely root causes
            2. Specific diagnostic steps to confirm the root cause
            3. Recommended fixes or workarounds
            4. Links to relevant documentation
            """);

      return new McpPromptMessage("user", messageText.toString());
   }

   /**
    * Builds a prompt message for client setup.
    */
   private McpPromptMessage buildSetupClientPromptMessage(String clientType, String framework) {
      StringBuilder messageText = new StringBuilder();
      messageText.append("Help me set up an Infinispan client with the following requirements:\n\n");
      messageText.append("Client type: ").append(clientType).append("\n");

      if (framework != null && !framework.isEmpty()) {
         messageText.append("Framework: ").append(framework).append("\n");
      }

      messageText.append("\nPlease provide:\n");

      switch (clientType) {
         case "hotrod" -> messageText.append("""
               1. Maven/Gradle dependency for the Hot Rod client
               2. Connection configuration (host, port, authentication)
               3. Code example for basic CRUD operations
               4. Marshalling setup (ProtoStream recommended)
               5. Connection pooling and timeout configuration
               6. Near-caching configuration for improved read performance
               """);
         case "rest" -> messageText.append("""
               1. REST API endpoint format and base URL
               2. Authentication setup (Basic, Bearer, Digest)
               3. Code examples for CRUD operations using HTTP client
               4. Content-Type headers and data format options (JSON, XML, Protobuf)
               5. Bulk operations and query API usage via REST
               """);
         case "embedded" -> messageText.append("""
               1. Maven/Gradle dependency for embedded cache
               2. Programmatic cache manager configuration
               3. Code example for creating and using caches
               4. Clustering setup for embedded mode
               5. Lifecycle management (start/stop cache manager)
               """);
         default -> messageText.append("Unknown client type. Valid values: hotrod, rest, embedded.\n");
      }

      if (framework != null && !framework.isEmpty()) {
         switch (framework) {
            case "spring" -> messageText.append("""

                  Spring Boot integration:
                  - Add infinispan-spring-boot3-starter-remote dependency
                  - Configure application.properties/yml with connection details
                  - Use @Cacheable, @CachePut, @CacheEvict annotations
                  - Configure Spring Cache Manager bean
                  """);
            case "quarkus" -> messageText.append("""

                  Quarkus integration:
                  - Add quarkus-infinispan-client extension
                  - Configure application.properties with quarkus.infinispan-client.* properties
                  - Use @Remote annotation for cache injection
                  - Use Dev Services for automatic server provisioning in dev mode
                  """);
            case "cdi" -> messageText.append("""

                  CDI integration:
                  - Add infinispan-cdi-embedded or infinispan-cdi-remote dependency
                  - Use @ConfigureCache annotation for cache configuration
                  - Inject RemoteCacheManager or EmbeddedCacheManager
                  - Use CDI events for cache notifications
                  """);
            default -> messageText.append("\nUnknown framework. Valid values: spring, quarkus, cdi.\n");
         }
      }

      return new McpPromptMessage("user", messageText.toString());
   }

   /**
    * Builds a prompt message for cross-site replication setup.
    */
   private McpPromptMessage buildSetupCrossSitePromptMessage(String topology, String sites) {
      StringBuilder messageText = new StringBuilder();
      messageText.append("Help me set up Infinispan cross-site replication with the following requirements:\n\n");
      messageText.append("Topology: ").append(topology).append("\n");

      if (sites != null && !sites.isEmpty()) {
         messageText.append("Number of sites: ").append(sites).append("\n");
      }

      messageText.append("\nTopology explanation:\n");
      switch (topology) {
         case "active-active" -> messageText.append("""
               - Active-Active: All sites handle read and write operations simultaneously.
               - Data is replicated bidirectionally between sites.
               - Conflict resolution must be configured (e.g., using XSiteEntryMergePolicy).
               - Provides the best availability but requires careful conflict handling.
               - Use synchronous backup strategy for strong consistency, async for better performance.
               """);
         case "active-passive" -> messageText.append("""
               - Active-Passive: One site handles all writes, other sites serve as hot standby.
               - Data flows unidirectionally from the active to the passive site(s).
               - Simpler to configure since there are no write conflicts.
               - Failover requires promoting a passive site to active.
               - Use synchronous backup strategy if zero data loss is required.
               """);
         default -> messageText.append("Unknown topology. Valid values: active-active, active-passive.\n");
      }

      messageText.append("""

            Please provide:
            1. JGroups RELAY2 configuration for cross-site communication
            2. Cache backup configuration (XML and YAML)
            3. Backup strategy recommendations (sync vs async) with trade-offs
            4. State transfer configuration for initial site synchronization
            5. Failure handling and take-offline configuration
            6. Monitoring and health checking for cross-site replication
            7. Documentation reference: https://infinispan.org/docs/stable/titles/xsite/xsite.html
            """);

      return new McpPromptMessage("user", messageText.toString());
   }

   /**
    * Builds a prompt message for documentation search.
    */
   private McpPromptMessage buildDocumentationPromptMessage(String topic, String context) {
      StringBuilder messageText = new StringBuilder();
      messageText.append("Search the Infinispan documentation for information about: ").append(topic);

      if (context != null && !context.isEmpty()) {
         messageText.append("\n\nAdditional context: ").append(context);
      }

      messageText.append("""
                  Documentation structure, list of topics and relative URL to search for:
                  - Configuring caches: https://infinispan.org/docs/stable/titles/configuring/configuring.html
                  - Encoding and marshalling data: https://infinispan.org/docs/stable/titles/encoding/encoding.html
                  - Querying caches: https://infinispan.org/docs/stable/titles/query/query.html
                  - Embedded: https://infinispan.org/docs/stable/titles/embedding/embedding.html
                  - Hot Rod client: https://infinispan.org/docs/stable/titles/hotrod_java/hotrod_java.html
                  - REST API: https://infinispan.org/docs/stable/titles/rest/rest.html
                  - Redis clients: https://infinispan.org/docs/stable/titles/resp/resp-endpoint.html
                  - Memcached clients: https://infinispan.org/docs/stable/titles/memcached/memcached.html
                  - Hibernate ORM cache provider: https://infinispan.org/docs/stable/titles/hibernate/hibernate.html
                  - Quarkus: https://quarkus.io/guides/infinispan-client
                  - Spring Boot: https://infinispan.org/docs/stable/titles/spring_boot/starter.html
                  - Server: https://infinispan.org/docs/stable/titles/server/server.html
                  - Operator: https://infinispan.org/docs/infinispan-operator/main/operator.html
                  - Helm Chart: https://infinispan.org/docs/helm-chart/main/helm-chart.html
                  - Ansible Collection: https://github.com/ansible-middleware/infinispan
                  - Command-Line Interface: https://infinispan.org/docs/stable/titles/cli/cli.html
                  - Planning and Tuning: https://infinispan.org/docs/stable/titles/tuning/tuning.html
                  - Cross-site replication: https://infinispan.org/docs/stable/titles/xsite/xsite.html
                  - Quarkus Langchain Extension: https://docs.quarkiverse.io/quarkus-langchain4j/dev/infinispan-store.html
                  - Langchain: https://python.langchain.com/docs/integrations/vectorstores/infinispanvs
                  - Langchain4j: https://docs.langchain4j.dev/integrations/embedding-stores/infinispan
                  - Vert.x Cluster Manager: https://vertx.io/docs/vertx-infinispan/java/
                  - Vert.x Web Sessions: https://how-to.vertx.io/web-session-infinispan-howto/
                  - Keycloak: https://www.keycloak.org/server/caching
                  - Apache Camel: https://camel.apache.org/components/latest/infinispan-component.html
                  - Wildfly: https://www.wildfly.org/

                  Please search the appropriate documentation URL for the provided topic and provide:
                  1. A brief summary of the topic
                  2. Direct links to relevant documentation pages
                  3. Code examples if available
                  4. Related topics that might be helpful""");

      return new McpPromptMessage("user", messageText.toString());
   }

   /**
    * Parses query parameters from a URI query string.
    * Example: "lines=200&level=ERROR" -> Map{lines=200, level=ERROR}
    */
   private Map<String, String> parseQueryParameters(String query) {
      if (query == null || query.isEmpty()) {
         return Map.of();
      }
      Map<String, String> params = new java.util.HashMap<>();
      for (String param : query.split("&")) {
         String[] pair = param.split("=", 2);
         if (pair.length == 2) {
            params.put(pair[0], pair[1]);
         }
      }
      return params;
   }

   /**
    * Maps log type to actual log file name.
    */
   private String getLogFileName(String logType) {
      return switch (logType == null ? "server" : logType) {
         case "server" -> "server.log";
         case "audit" -> "audit.log";
         case "rest-access" -> "rest-access.log";
         case "hotrod-access" -> "hotrod-access.log";
         case "memcached-access" -> "memcached-access.log";
         case "resp-access" -> "resp-access.log";
         case "gc" -> "gc.log";
         default -> throw new IllegalArgumentException("Unknown log type: " + logType);
      };
   }

   /**
    * Reads the last N lines from a file efficiently using reverse file reading.
    * This approach minimizes memory usage for large files.
    */
   private String readLastLines(java.nio.file.Path file, int maxLines) throws java.io.IOException {
      if (!java.nio.file.Files.exists(file)) {
         return ""; // Return empty string if file doesn't exist yet
      }

      long fileSize = java.nio.file.Files.size(file);
      if (fileSize == 0) {
         return "";
      }

      // For small files, just read the whole thing
      if (fileSize < 8192) {
         return java.nio.file.Files.readString(file);
      }

      // For larger files, read backwards in chunks
      java.util.List<String> lines = new java.util.ArrayList<>();
      try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(file.toFile(), "r")) {
         long pos = fileSize - 1;
         StringBuilder currentLine = new StringBuilder();
         int chunkSize = 8192;
         byte[] buffer = new byte[chunkSize];

         while (pos >= 0 && lines.size() < maxLines) {
            int bytesToRead = (int) Math.min(chunkSize, pos + 1);
            pos = pos - bytesToRead + 1;
            raf.seek(pos);
            raf.readFully(buffer, 0, bytesToRead);

            // Process bytes in reverse
            for (int i = bytesToRead - 1; i >= 0; i--) {
               char c = (char) buffer[i];
               if (c == '\n') {
                  if (currentLine.length() > 0) {
                     lines.add(currentLine.reverse().toString());
                     currentLine = new StringBuilder();
                     if (lines.size() >= maxLines) {
                        break;
                     }
                  }
               } else if (c != '\r') {
                  currentLine.append(c);
               }
            }
            pos--;
         }

         // Add any remaining content as the first line
         if (currentLine.length() > 0 && lines.size() < maxLines) {
            lines.add(currentLine.reverse().toString());
         }
      }

      // Reverse the list to get correct order (oldest to newest)
      java.util.Collections.reverse(lines);
      return String.join("\n", lines);
   }
}
