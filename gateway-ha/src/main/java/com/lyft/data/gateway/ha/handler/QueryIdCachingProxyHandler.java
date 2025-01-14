package com.lyft.data.gateway.ha.handler;

import com.codahale.metrics.Meter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.google.common.net.HttpHeaders;
import com.lyft.data.gateway.ha.router.QueryHistoryManager;
import com.lyft.data.gateway.ha.router.RoutingManager;
import com.lyft.data.proxyserver.ProxyHandler;
import com.lyft.data.proxyserver.wrapper.MultiReadHttpServletRequest;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.Callback;

@Slf4j
public class QueryIdCachingProxyHandler extends ProxyHandler {
  public static final String PROXY_TARGET_HEADER = "proxytarget";
  public static final String V1_STATEMENT_PATH = "/v1/statement";
  public static final String V1_QUERY_PATH = "/v1/query";
  public static final String V1_INFO_PATH = "/v1/info";
  public static final String UI_API_STATS_PATH = "/ui/api/stats";
  public static final String PRESTO_UI_PATH = "/ui";

  // Add support for Trino
  public static final String USER_HEADER = "X-Trino-User";
  public static final String ALTERNATE_USER_HEADER = "X-Presto-User";
  public static final String SOURCE_HEADER = "X-Trino-Source";
  public static final String ALTERNATE_SOURCE_HEADER = "X-Presto-Source";
  public static final String ROUTING_GROUP_HEADER = "X-Trino-Routing-Group";
  public static final String ALTERNATE_ROUTING_GROUP_HEADER = "X-Presto-Routing-Group";
  public static final String CLIENT_TAGS_HEADER = "X-Trino-Client-Tags";
  public static final String ALTERNATE_CLIENT_TAGS_HEADER = "X-Presto-Client-Tags";
  public static final String ADHOC_ROUTING_GROUP = "adhoc";
  private static final int QUERY_TEXT_LENGTH_FOR_HISTORY = 200;

  private static final Pattern EXTRACT_BETWEEN_SINGLE_QUOTES = Pattern.compile("'([^\\s']+)'");

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final RoutingManager routingManager;
  private final QueryHistoryManager queryHistoryManager;

  private final Meter requestMeter;

  public QueryIdCachingProxyHandler(
      QueryHistoryManager queryHistoryManager,
      RoutingManager routingManager,
      int serverApplicationPort,
      Meter requestMeter) {
    super(serverApplicationPort);
    this.requestMeter = requestMeter;
    this.routingManager = routingManager;
    this.queryHistoryManager = queryHistoryManager;
  }

  @Override
  public void preConnectionHook(HttpServletRequest request, Request proxyRequest) {
    if (request.getMethod().equals(HttpMethod.POST)
        && request.getRequestURI().startsWith(V1_STATEMENT_PATH)) {
      requestMeter.mark();
      try {
        String requestBody = CharStreams.toString(request.getReader());
        // 6sense: Changed to debug to save sumo logic expenses
        log.debug(
            "Processing request endpoint: [{}], payload: [{}]",
            request.getRequestURI(),
            requestBody);
        debugLogHeaders(request);
      } catch (Exception e) {
        log.warn("Error fetching the request payload", e);
      }
    }
  }

  private boolean isPathWhiteListed(String path) {
    return path.startsWith(V1_STATEMENT_PATH)
        || path.startsWith(V1_QUERY_PATH)
        || path.startsWith(PRESTO_UI_PATH)
        || path.startsWith(V1_INFO_PATH)
        || path.startsWith(UI_API_STATS_PATH);
  }

  public boolean isAuthEnabled() {
    return false;
  }

  public boolean handleAuthRequest(HttpServletRequest request) {
    return true;
  }

  @Override
  public String rewriteTarget(HttpServletRequest request) {
    /* Here comes the load balancer / gateway */
    String backendAddress = "http://localhost:" + serverApplicationPort;

    // Only load balance presto query APIs.
    if (isPathWhiteListed(request.getRequestURI())) {
      String queryId = extractQueryIdIfPresent(request);

      // Find query id and get url from cache
      if (!Strings.isNullOrEmpty(queryId)) {
        backendAddress = routingManager.findBackendForQueryId(queryId);
      } else {
        String routingGroup = Optional.ofNullable(request.getHeader(ROUTING_GROUP_HEADER))
            .orElse(request.getHeader(ALTERNATE_ROUTING_GROUP_HEADER));
        // Fall back on client tags for routing
        if (Strings.isNullOrEmpty(routingGroup)) {
          routingGroup = Optional.ofNullable(request.getHeader(CLIENT_TAGS_HEADER))
            .orElse(request.getHeader(ALTERNATE_CLIENT_TAGS_HEADER));
        }
        if (!Strings.isNullOrEmpty(routingGroup)) {
          // This falls back on adhoc backend if there are no cluster found for the routing group.
          backendAddress = routingManager.provideBackendForRoutingGroup(routingGroup);
        } else {
          backendAddress = routingManager.provideAdhocBackend();
        }
      }
      // set target backend so that we could save queryId to backend mapping later.
      ((MultiReadHttpServletRequest) request).addHeader(PROXY_TARGET_HEADER, backendAddress);
    }
    if (isAuthEnabled() && request.getHeader("Authorization") != null) {
      if (!handleAuthRequest(request)) {
        // This implies the AuthRequest was not authenticated, hence we error out from here.
        log.info("Could not authenticate Request: " + request.toString());
        return null;
      }
    }
    String targetLocation =
        backendAddress
            + request.getRequestURI()
            + (request.getQueryString() != null ? "?" + request.getQueryString() : "");

    String originalLocation =
        request.getScheme()
            + "://"
            + request.getRemoteHost()
            + ":"
            + request.getServerPort()
            + request.getRequestURI()
            + (request.getQueryString() != null ? "?" + request.getQueryString() : "");

    // 6sense: Changed to debug to save sumo logic expenses
    log.debug("Rerouting [{}]--> [{}]", originalLocation, targetLocation);
    return targetLocation;
  }

  /**
   * Extracts the queryId from the server requests if it is present.
   * @param request The request made
   * @return The queryId (if present)
   */
  protected String extractQueryIdIfPresent(HttpServletRequest request) {
    String path = request.getRequestURI();
    String queryParams = request.getQueryString();
    try {
      String queryText = CharStreams.toString(request.getReader());
      if (!Strings.isNullOrEmpty(queryText)
          && queryText.toLowerCase().contains("system.runtime.kill_query")) {
        // extract and return the queryId
        String[] parts = queryText.split(",");
        for (String part : parts) {
          if (part.contains("query_id")) {
            Matcher m = EXTRACT_BETWEEN_SINGLE_QUOTES.matcher(part);
            if (m.find()) {
              String queryQuoted = m.group();
              if (!Strings.isNullOrEmpty(queryQuoted) && queryQuoted.length() > 0) {
                return queryQuoted.substring(1, queryQuoted.length() - 1);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Error extracting query payload from request", e);
    }
    
    log.debug("Trying to extract query id from path [{}] or queryString [{}]", path, queryParams);
    String queryId = extractQueryIdIfPresent(path, queryParams);
    if (queryId == null && !Strings.isNullOrEmpty(request.getHeader(HttpHeaders.REFERER))) {
      log.debug("Trying to extract query id from referer [{}]", 
          request.getHeader(HttpHeaders.REFERER));
      queryId = extractQueryIdFromReferer(request.getHeader(HttpHeaders.REFERER));
    }

    log.debug("query id in url [{}]", queryId);
    return queryId;
  }

  /**
   * Extracts the queryId from the path of a request and the parameters of the query.
   * @param path Path of request
   * @param queryParams Query parameters
   * @return The queryId (if present)
   */
  protected String extractQueryIdIfPresent(String path, String queryParams) {
    if (path == null) {
      return null;
    }

    String queryId = null;

    if (path.startsWith(V1_STATEMENT_PATH) || path.startsWith(V1_QUERY_PATH)) {
      String[] tokens = path.split("/");
      if (tokens.length >= 4) {
        if (path.contains("queued")
            || path.contains("scheduled")
            || path.contains("executing")
            || path.contains("partialCancel")) {
          queryId = tokens[4];
        } else {
          queryId = tokens[3];
        }
      }
    } else if (path.startsWith(PRESTO_UI_PATH)) {
      queryId = queryParams;
    }

    return queryId;
  }

  /**
   * Extracts the queryId from the referer of the request.
   * @param referer Request referer
   * @return The queryId of the request
   */
  protected String extractQueryIdFromReferer(String referer) {
    try {
      URL refUrl = new URL(referer);
      if (refUrl.getPath().startsWith(PRESTO_UI_PATH)) {
        return refUrl.getQuery();
      }
    } catch (Exception e) {
      log.debug("Unable to extract query id from referer");
    }

    return null;
  }

  protected void postConnectionHook(
      HttpServletRequest request,
      HttpServletResponse response,
      byte[] buffer,
      int offset,
      int length,
      Callback callback) {
    try {
      String requestPath = request.getRequestURI();
      if (requestPath.startsWith(V1_STATEMENT_PATH)
          && request.getMethod().equals(HttpMethod.POST)) {
        String output;
        boolean isGZipEncoding = isGZipEncoding(response);
        if (isGZipEncoding) {
          output = plainTextFromGz(buffer);
        } else {
          output = new String(buffer);
        }
        log.debug("Response output [{}]", output);

        QueryHistoryManager.QueryDetail queryDetail = getQueryDetailsFromRequest(request);
        log.debug("Proxy destination : {}", queryDetail.getBackendUrl());

        if (response.getStatus() == HttpStatus.OK_200) {
          HashMap<String, String> results = OBJECT_MAPPER.readValue(output, HashMap.class);
          queryDetail.setQueryId(results.get("id"));

          if (!Strings.isNullOrEmpty(queryDetail.getQueryId())) {
            routingManager.setBackendForQueryId(
                queryDetail.getQueryId(), queryDetail.getBackendUrl());

            // Saving history at gateway.
            queryHistoryManager.submitQueryDetail(queryDetail);
            
            log.debug(
                "QueryId [{}] mapped with proxy [{}]",
                queryDetail.getQueryId(),
                queryDetail.getBackendUrl());
          } else {
            log.debug("QueryId [{}] could not be cached", queryDetail.getQueryId());
          }
        } else {
          log.error(
              "Non OK HTTP Status code with response [{}] , Status code [{}]",
              output,
              response.getStatus());
        }
      } else {
        log.debug("SKIPPING For {}", requestPath);
      }
    } catch (Exception e) {
      log.error("Error in proxying falling back to super call", e);
    }
    super.postConnectionHook(request, response, buffer, offset, length, callback);
  }

  private QueryHistoryManager.QueryDetail getQueryDetailsFromRequest(HttpServletRequest request)
      throws IOException {
    QueryHistoryManager.QueryDetail queryDetail = new QueryHistoryManager.QueryDetail();
    queryDetail.setBackendUrl(request.getHeader(PROXY_TARGET_HEADER));
    queryDetail.setCaptureTime(System.currentTimeMillis());
    queryDetail.setUser(Optional.ofNullable(request.getHeader(USER_HEADER))
            .orElse(request.getHeader(ALTERNATE_USER_HEADER)));
    queryDetail.setSource(Optional.ofNullable(request.getHeader(SOURCE_HEADER))
            .orElse(request.getHeader(ALTERNATE_SOURCE_HEADER)));
    String queryText = CharStreams.toString(request.getReader());
    queryDetail.setQueryText(
        queryText.length() > QUERY_TEXT_LENGTH_FOR_HISTORY
            ? queryText.substring(0, QUERY_TEXT_LENGTH_FOR_HISTORY) + "..."
            : queryText);
    return queryDetail;
  }
}
