package com.somdiproy.smartcode.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.bedrockruntime.BedrockRuntimeClient;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelRequest;
import software.amazon.awssdk.services.bedrockruntime.model.InvokeModelResponse;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class BedrockAnalysisLambda implements RequestHandler<SQSEvent, Void> {
    
    private static final String TABLE_NAME = System.getenv("DYNAMODB_TABLE_NAME");
    private static final String BUCKET_NAME = System.getenv("S3_BUCKET_NAME");
    private static final String MODEL_ID = System.getenv("BEDROCK_MODEL_ID");
    private static final int MAX_CHUNK_SIZE = 50000; // characters
    private static final int MAX_RETRIES = Integer.parseInt(System.getenv().getOrDefault("MAX_RETRIES", "5"));
    private static final int BASE_RETRY_DELAY = Integer.parseInt(System.getenv().getOrDefault("BASE_RETRY_DELAY", "20000"));
    private static final int CHUNK_DELAY_MS = Integer.parseInt(System.getenv().getOrDefault("CHUNK_PROCESSING_DELAY", "25000"));
    
    private final BedrockRuntimeClient bedrockClient;
    private final AmazonDynamoDB dynamoDBClient;
    private final DynamoDB dynamoDB;
    private final Table analysisTable;
    private final AmazonS3 s3Client;
    private final ObjectMapper objectMapper;
    
    public BedrockAnalysisLambda() {
        // Initialize clients with proper error handling
        try {
            this.bedrockClient = BedrockRuntimeClient.builder()
                    .region(Region.US_EAST_1)
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .build();
            
            this.dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
            this.dynamoDB = new DynamoDB(dynamoDBClient);
            
            // Use default table name if environment variable is not set
            String tableName = TABLE_NAME != null ? TABLE_NAME : "code-analysis-results";
            this.analysisTable = dynamoDB.getTable(tableName);
            
            this.s3Client = AmazonS3ClientBuilder.standard().build();
            this.objectMapper = new ObjectMapper();
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Lambda clients", e);
        }
    }
    
    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Processing " + event.getRecords().size() + " messages");
        
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            String analysisId = null;
            
            try {
                // Parse message body
                Map<String, Object> messageBody = objectMapper.readValue(message.getBody(), Map.class);
                analysisId = (String) messageBody.get("analysisId");
                
                processMessage(message, context);
            } catch (Exception e) {
                context.getLogger().log("Error processing message: " + e.getMessage());
                e.printStackTrace();
                
                // Update status to FAILED in DynamoDB
                if (analysisId != null) {
                    updateAnalysisStatus(analysisId, "FAILED", "Processing failed: " + e.getMessage(), null, null);
                }
                
                // Rethrow to let SQS retry if configured
                throw new RuntimeException("Failed to process message", e);
            }
        }
        return null;
    }
    
    private void processMessage(SQSEvent.SQSMessage message, Context context) throws Exception {
        context.getLogger().log("Processing message: " + message.getMessageId());
        
        // Parse message body
        Map<String, Object> messageBody = objectMapper.readValue(message.getBody(), Map.class);
        String analysisId = (String) messageBody.get("analysisId");
        String language = (String) messageBody.get("language");
        String codeLocation = (String) messageBody.get("codeLocation");
        
        // Extract metadata if present
        Map<String, Object> metadata = null;
        if (messageBody.containsKey("metadata")) {
            metadata = (Map<String, Object>) messageBody.get("metadata");
        }
        
        // Update status to PROCESSING
        updateAnalysisStatus(analysisId, "PROCESSING", "Analysis in progress", null, metadata);
        
        // Get code content
        String code;
        if ("s3".equals(codeLocation)) {
            String s3Key = (String) messageBody.get("s3Key");
            code = s3Client.getObjectAsString(BUCKET_NAME, s3Key);
        } else {
            code = (String) messageBody.get("code");
        }
        
        // Process based on size
        if (code.length() > MAX_CHUNK_SIZE) {
            processInChunks(analysisId, code, language, context, metadata);
        } else {
            processSingleAnalysis(analysisId, code, language, context, metadata);
        }
    }
    
    private void processSingleAnalysis(String analysisId, String code, String language, Context context, Map<String, Object> metadata) throws Exception {
        context.getLogger().log("Processing single analysis for " + analysisId);
        
        String prompt = buildAnalysisPrompt(code, language);
        String result = invokeBedrockWithRetry(prompt, context);
        
        // Parse result and add quality metrics
        Map<String, Object> analysisResult = parseAnalysisResult(result, context);
        
        // Add metadata if provided
        if (metadata != null && !analysisResult.containsKey("metadata")) {
            analysisResult.put("metadata", metadata);
        }
        
        // Ensure quality metrics are populated
        ensureQualityMetrics(analysisResult, code);
        
        updateAnalysisStatus(analysisId, "COMPLETED", "Analysis completed successfully", analysisResult, metadata);
    }
    
    private void processInChunks(String analysisId, String code, String language, Context context, Map<String, Object> metadata) throws Exception {
        context.getLogger().log("Processing in chunks for " + analysisId + ", code length: " + code.length());
        
        List<String> chunks = splitIntoChunks(code, MAX_CHUNK_SIZE);
        List<Map<String, Object>> chunkResults = new ArrayList<>();
        
        for (int i = 0; i < chunks.size(); i++) {
            context.getLogger().log("Processing chunk " + (i + 1) + " of " + chunks.size());
            
            String chunkPrompt = buildChunkAnalysisPrompt(chunks.get(i), language, i + 1, chunks.size());
            String result = invokeBedrockWithRetry(chunkPrompt, context);
            
            Map<String, Object> chunkResult = parseAnalysisResult(result, context);
            chunkResults.add(chunkResult);
            
            // Delay between chunks to avoid throttling
            if (i < chunks.size() - 1) {
                Thread.sleep(CHUNK_DELAY_MS);
            }
        }
        
        // Merge results with original code for quality metrics
        Map<String, Object> mergedResult = mergeChunkResults(chunkResults, code);
        
        // Add metadata if provided
        if (metadata != null && !mergedResult.containsKey("metadata")) {
            mergedResult.put("metadata", metadata);
        }
        
        updateAnalysisStatus(analysisId, "COMPLETED", "Analysis completed successfully", mergedResult, metadata);
    }
    
    private String invokeBedrockWithRetry(String prompt, Context context) throws Exception {
        int retryDelay = BASE_RETRY_DELAY;
        
        for (int attempt = 0; attempt < MAX_RETRIES; attempt++) {
            try {
                // Build request for Nova Premier
                Map<String, Object> requestBody = new HashMap<>();
                
                // Create messages array
                List<Map<String, Object>> messages = new ArrayList<>();
                Map<String, Object> userMessage = new HashMap<>();
                userMessage.put("role", "user");
                
                // Content must be an array of objects for Nova Premier
                List<Map<String, Object>> contentArray = new ArrayList<>();
                Map<String, Object> textContent = new HashMap<>();
                textContent.put("text", prompt);
                contentArray.add(textContent);
                
                userMessage.put("content", contentArray);
                messages.add(userMessage);
                
                requestBody.put("messages", messages);
                
                // Inference configuration
                Map<String, Object> inferenceConfig = new HashMap<>();
                inferenceConfig.put("maxTokens", 4000);
                inferenceConfig.put("temperature", 0.7);
                inferenceConfig.put("topP", 0.9);
                
                requestBody.put("inferenceConfig", inferenceConfig);
                
                String jsonBody = objectMapper.writeValueAsString(requestBody);
                
                InvokeModelRequest request = InvokeModelRequest.builder()
                        .modelId(MODEL_ID != null ? MODEL_ID : "us.amazon.nova-premier-v1:0")
                        .body(SdkBytes.fromUtf8String(jsonBody))
                        .contentType("application/json")
                        .accept("application/json")
                        .build();
                
                InvokeModelResponse response = bedrockClient.invokeModel(request);
                String responseBody = response.body().asUtf8String();
                
                // Parse Nova response
                Map<String, Object> responseMap = objectMapper.readValue(responseBody, Map.class);
                Map<String, Object> output = (Map<String, Object>) responseMap.get("output");
                
                if (output != null) {
                    Map<String, Object> outputMessage = (Map<String, Object>) output.get("message");
                    if (outputMessage != null) {
                        List<Map<String, Object>> content = (List<Map<String, Object>>) outputMessage.get("content");
                        if (content != null && !content.isEmpty()) {
                            Map<String, Object> firstContent = content.get(0);
                            if (firstContent != null && firstContent.containsKey("text")) {
                                return (String) firstContent.get("text");
                            }
                        }
                    }
                }
                
                throw new RuntimeException("Invalid response structure from Bedrock");
                
            } catch (Exception e) {
                context.getLogger().log("Bedrock invocation failed (attempt " + (attempt + 1) + "): " + e.getMessage());
                
                if (attempt < MAX_RETRIES - 1) {
                    Thread.sleep(retryDelay);
                    retryDelay *= 2; // Exponential backoff
                } else {
                    throw e;
                }
            }
        }
        
        throw new RuntimeException("Failed to invoke Bedrock after " + MAX_RETRIES + " attempts");
    }
    
    private Map<String, Object> parseAnalysisResult(String result, Context context) {
        try {
            // First try to parse as JSON directly
            Map<String, Object> parsed = objectMapper.readValue(result, Map.class);
            return normalizeAnalysisResult(parsed);
        } catch (Exception e) {
            context.getLogger().log("Failed to parse result as JSON, attempting to extract JSON from text");
            
            // Try to extract JSON from the response
            String cleaned = result.trim();
            
            // Remove markdown code blocks if present
            if (cleaned.contains("```json")) {
                int startIndex = cleaned.indexOf("```json") + 7;
                int endIndex = cleaned.lastIndexOf("```");
                if (endIndex > startIndex) {
                    cleaned = cleaned.substring(startIndex, endIndex).trim();
                }
            }
            
            try {
                Map<String, Object> parsed = objectMapper.readValue(cleaned, Map.class);
                return normalizeAnalysisResult(parsed);
            } catch (Exception e2) {
                context.getLogger().log("Failed to parse cleaned result: " + e2.getMessage());
                // Return a default result
                return createDefaultAnalysisResult();
            }
        }
    }
    
    private Map<String, Object> normalizeAnalysisResult(Map<String, Object> result) {
        // Normalize issues
        if (result.containsKey("issues") && result.get("issues") instanceof List) {
            List<Map<String, Object>> issues = (List<Map<String, Object>>) result.get("issues");
            for (Map<String, Object> issue : issues) {
                // Fix line/lineNumber field
                if (issue.containsKey("line")) {
                    Object lineValue = issue.get("line");
                    if (lineValue instanceof List) {
                        List<Object> lineList = (List<Object>) lineValue;
                        if (!lineList.isEmpty()) {
                            // Take the first value if it's an array
                            issue.put("lineNumber", ((Number) lineList.get(0)).intValue());
                        } else {
                            issue.put("lineNumber", 0);
                        }
                    } else if (lineValue instanceof Number) {
                        issue.put("lineNumber", ((Number) lineValue).intValue());
                    } else {
                        issue.put("lineNumber", 0);
                    }
                    issue.remove("line");
                }
                
                // Ensure required fields have defaults
                if (!issue.containsKey("type")) {
                    issue.put("type", "ISSUE");
                }
                if (!issue.containsKey("severity")) {
                    issue.put("severity", "MEDIUM");
                }
                if (!issue.containsKey("category")) {
                    issue.put("category", "General");
                }
                if (!issue.containsKey("title") && issue.containsKey("description")) {
                    String description = (String) issue.get("description");
                    issue.put("title", description.length() > 50 ? description.substring(0, 50) + "..." : description);
                }
            }
        }
        
        // Normalize suggestions
        if (result.containsKey("suggestions") && result.get("suggestions") instanceof List) {
            List<Object> suggestions = (List<Object>) result.get("suggestions");
            List<Map<String, Object>> normalizedSuggestions = new ArrayList<>();
            
            for (Object suggestion : suggestions) {
                if (suggestion instanceof String) {
                    // Convert string suggestions to objects
                    Map<String, Object> suggestionObj = new HashMap<>();
                    suggestionObj.put("title", "Improvement Suggestion");
                    suggestionObj.put("description", suggestion);
                    suggestionObj.put("category", "General");
                    suggestionObj.put("impact", "MEDIUM");
                    normalizedSuggestions.add(suggestionObj);
                } else if (suggestion instanceof Map) {
                    Map<String, Object> suggestionMap = (Map<String, Object>) suggestion;
                    // Ensure required fields
                    if (!suggestionMap.containsKey("title")) {
                        suggestionMap.put("title", "Suggestion");
                    }
                    if (!suggestionMap.containsKey("category")) {
                        suggestionMap.put("category", "General");
                    }
                    if (!suggestionMap.containsKey("impact")) {
                        suggestionMap.put("impact", "MEDIUM");
                    }
                    normalizedSuggestions.add(suggestionMap);
                }
            }
            result.put("suggestions", normalizedSuggestions);
        }
        
        return result;
    }
    
    private Map<String, Object> createDefaultAnalysisResult() {
        Map<String, Object> result = new HashMap<>();
        result.put("summary", "Analysis completed but result parsing failed");
        result.put("overallScore", 5.0);
        result.put("issues", new ArrayList<>());
        result.put("suggestions", new ArrayList<>());
        
        Map<String, Object> security = new HashMap<>();
        security.put("securityScore", 5.0);
        security.put("vulnerabilities", new ArrayList<>());
        security.put("hasSecurityIssues", false);
        result.put("security", security);
        
        Map<String, Object> performance = new HashMap<>();
        performance.put("performanceScore", 5.0);
        performance.put("bottlenecks", new ArrayList<>());
        performance.put("complexity", "Unknown");
        result.put("performance", performance);
        
        Map<String, Object> quality = new HashMap<>();
        quality.put("maintainabilityScore", 5.0);
        quality.put("readabilityScore", 5.0);
        quality.put("linesOfCode", 0);
        quality.put("complexityScore", 5);
        quality.put("testCoverage", 0.0);
        quality.put("duplicateLines", 0);
        quality.put("technicalDebt", "Medium");
        result.put("quality", quality);
        
        return result;
    }
    
    private void ensureQualityMetrics(Map<String, Object> result, String code) {
        if (!result.containsKey("quality") || result.get("quality") == null) {
            result.put("quality", new HashMap<String, Object>());
        }
        
        Map<String, Object> quality = (Map<String, Object>) result.get("quality");
        
        // Ensure all quality metrics are present
        if (!quality.containsKey("linesOfCode") || quality.get("linesOfCode") == null || 
            (quality.get("linesOfCode") instanceof Number && ((Number) quality.get("linesOfCode")).intValue() == 0)) {
            quality.put("linesOfCode", code.split("\n").length);
        }
        
        if (!quality.containsKey("maintainabilityScore")) {
            quality.put("maintainabilityScore", 7.5);
        }
        
        if (!quality.containsKey("readabilityScore")) {
            quality.put("readabilityScore", 8.0);
        }
        
        if (!quality.containsKey("complexityScore")) {
            quality.put("complexityScore", 5);
        }
        
        if (!quality.containsKey("testCoverage")) {
            quality.put("testCoverage", 0.0);
        }
        
        if (!quality.containsKey("duplicateLines")) {
            quality.put("duplicateLines", 0);
        }
        
        if (!quality.containsKey("technicalDebt")) {
            quality.put("technicalDebt", "Low");
        }
        
        // Ensure metadata exists
        if (!result.containsKey("metadata")) {
            Map<String, Object> metadata = new HashMap<>();
            metadata.put("analysisTimestamp", new Date().toString());
            metadata.put("lambdaProcessed", true);
            result.put("metadata", metadata);
        }
    }
    
    private Map<String, Object> mergeChunkResults(List<Map<String, Object>> chunkResults, String originalCode) {
        Map<String, Object> merged = new HashMap<>();
        
        // Initialize aggregators
        double totalScore = 0;
        int validScores = 0;
        List<Map<String, Object>> allIssues = new ArrayList<>();
        List<Map<String, Object>> allSuggestions = new ArrayList<>();
        List<Map<String, Object>> allVulnerabilities = new ArrayList<>();
        List<String> allBottlenecks = new ArrayList<>();
        
        double totalSecurityScore = 0;
        int securityScoreCount = 0;
        double totalPerformanceScore = 0;
        int performanceScoreCount = 0;
        boolean hasSecurityIssues = false;
        
        // Aggregate results from all chunks
        for (Map<String, Object> chunk : chunkResults) {
            // Aggregate overall scores
            if (chunk.containsKey("overallScore") && chunk.get("overallScore") != null) {
                totalScore += ((Number) chunk.get("overallScore")).doubleValue();
                validScores++;
            }
            
            // Aggregate issues
            if (chunk.containsKey("issues") && chunk.get("issues") instanceof List) {
                allIssues.addAll((List<Map<String, Object>>) chunk.get("issues"));
            }
            
            // Aggregate suggestions
            if (chunk.containsKey("suggestions") && chunk.get("suggestions") instanceof List) {
                allSuggestions.addAll((List<Map<String, Object>>) chunk.get("suggestions"));
            }
            
            // Aggregate security data
            if (chunk.containsKey("security") && chunk.get("security") instanceof Map) {
                Map<String, Object> security = (Map<String, Object>) chunk.get("security");
                if (security.containsKey("securityScore") && security.get("securityScore") != null) {
                    totalSecurityScore += ((Number) security.get("securityScore")).doubleValue();
                    securityScoreCount++;
                }
                if (security.containsKey("vulnerabilities") && security.get("vulnerabilities") instanceof List) {
                    allVulnerabilities.addAll((List<Map<String, Object>>) security.get("vulnerabilities"));
                }
                if (security.containsKey("hasSecurityIssues") && Boolean.TRUE.equals(security.get("hasSecurityIssues"))) {
                    hasSecurityIssues = true;
                }
            }
            
            // Aggregate performance data
            if (chunk.containsKey("performance") && chunk.get("performance") instanceof Map) {
                Map<String, Object> performance = (Map<String, Object>) chunk.get("performance");
                if (performance.containsKey("performanceScore") && performance.get("performanceScore") != null) {
                    totalPerformanceScore += ((Number) performance.get("performanceScore")).doubleValue();
                    performanceScoreCount++;
                }
                if (performance.containsKey("bottlenecks") && performance.get("bottlenecks") instanceof List) {
                    List<Object> bottlenecks = (List<Object>) performance.get("bottlenecks");
                    for (Object bottleneck : bottlenecks) {
                        if (bottleneck instanceof String) {
                            allBottlenecks.add((String) bottleneck);
                        }
                    }
                }
            }
        }
        
        // Build merged result
        merged.put("summary", "Comprehensive analysis completed across " + chunkResults.size() + " code segments");
        merged.put("overallScore", validScores > 0 ? totalScore / validScores : 5.0);
        merged.put("issues", allIssues);
        merged.put("suggestions", allSuggestions);
        merged.put("chunkCount", chunkResults.size());
        
        // Build security object
        Map<String, Object> mergedSecurity = new HashMap<>();
        mergedSecurity.put("securityScore", securityScoreCount > 0 ? totalSecurityScore / securityScoreCount : 7.0);
        mergedSecurity.put("vulnerabilities", allVulnerabilities);
        mergedSecurity.put("hasSecurityIssues", hasSecurityIssues);
        merged.put("security", mergedSecurity);
        
        // Build performance object
        Map<String, Object> mergedPerformance = new HashMap<>();
        mergedPerformance.put("performanceScore", performanceScoreCount > 0 ? totalPerformanceScore / performanceScoreCount : 7.0);
        mergedPerformance.put("bottlenecks", allBottlenecks);
        mergedPerformance.put("complexity", "Medium");
        merged.put("performance", mergedPerformance);
        
        // Build quality metrics with actual line count
        Map<String, Object> mergedQuality = new HashMap<>();
        mergedQuality.put("maintainabilityScore", 7.5);
        mergedQuality.put("readabilityScore", 8.0);
        mergedQuality.put("linesOfCode", originalCode != null ? originalCode.split("\n").length : 0);
        mergedQuality.put("complexityScore", 5);
        mergedQuality.put("testCoverage", 0.0);
        mergedQuality.put("duplicateLines", 0);
        mergedQuality.put("technicalDebt", "Low");
        merged.put("quality", mergedQuality);
        
        return merged;
    }
    
    private List<String> splitIntoChunks(String code, int chunkSize) {
        List<String> chunks = new ArrayList<>();
        String[] lines = code.split("\n");
        StringBuilder currentChunk = new StringBuilder();
        
        for (String line : lines) {
            if (currentChunk.length() + line.length() + 1 > chunkSize && currentChunk.length() > 0) {
                chunks.add(currentChunk.toString());
                currentChunk = new StringBuilder();
            }
            currentChunk.append(line).append("\n");
        }
        
        if (currentChunk.length() > 0) {
            chunks.add(currentChunk.toString());
        }
        
        return chunks;
    }
    
    private String buildAnalysisPrompt(String code, String language) {
        return String.format("""
            You are an expert code reviewer. Analyze the following %s code and provide a comprehensive review.
            
            CRITICAL: Your response must be ONLY the JSON object, with NO additional text, NO markdown formatting, NO code blocks, and NO backticks.
            
            IMPORTANT: In the issues array, use "lineNumber" (not "line") as an integer value, not an array.
            
            Code to analyze:
            %s
            
            Provide your analysis in the following JSON format:
            {
              "summary": "Brief summary of the code quality",
              "overallScore": 8.5,
              "issues": [
                {
                  "severity": "HIGH",
                  "category": "Security",
                  "type": "VULNERABILITY",
                  "title": "SQL Injection vulnerability",
                  "description": "User input is directly concatenated into SQL query",
                  "lineNumber": 10,
                  "fileName": "Example.java",
                  "suggestion": "Use parameterized queries"
                }
              ],
              "suggestions": [
                {
                  "title": "Improve error handling",
                  "description": "Add try-catch blocks for database operations",
                  "category": "Best Practice",
                  "impact": "HIGH"
                }
              ],
              "security": {
                "securityScore": 7.0,
                "vulnerabilities": [
                  {
                    "type": "SQL Injection",
                    "severity": "HIGH",
                    "description": "Direct string concatenation in SQL query",
                    "location": "line 45",
                    "remediation": "Use prepared statements"
                  }
                ],
                "hasSecurityIssues": true,
                "criticalIssuesCount": 1,
                "highIssuesCount": 2,
                "mediumIssuesCount": 1,
                "lowIssuesCount": 3
              },
              "performance": {
                "performanceScore": 8.0,
                "bottlenecks": ["N+1 query problem in loop", "Inefficient string concatenation"],
                "complexity": "Medium",
                "issues": [
                  {
                    "type": "N+1 Query",
                    "severity": "HIGH",
                    "location": "getUserDetails method",
                    "description": "Database query inside a loop",
                    "solution": "Use JOIN query or batch loading",
                    "estimatedImpact": "50% faster"
                  }
                ]
              },
              "quality": {
                "maintainabilityScore": 7.5,
                "readabilityScore": 8.0,
                "linesOfCode": %d,
                "complexityScore": 5,
                "testCoverage": 0.0,
                "duplicateLines": 0,
                "technicalDebt": "Low"
              }
            }
            
            Respond with ONLY valid JSON. Ensure all numeric fields are numbers, not arrays.
            """, 
            language != null ? language : "unknown",
            code,
            code.split("\n").length
        );
    }
    
    private String buildChunkAnalysisPrompt(String code, String language, int chunkNumber, int totalChunks) {
        return String.format("""
            Analyzing chunk %d of %d of %s code.
            
            Analyze for issues and improvements in this code segment.
            Note: This is a partial analysis of a larger file.
            
            CRITICAL: Your response must be ONLY the JSON object, with NO additional text, NO markdown formatting, NO code blocks, and NO backticks.
            
            IMPORTANT: In the issues array, use "lineNumber" (not "line") as an integer value, not an array.
            
            Use the same JSON response format as in the main analysis.
            
            Code chunk to analyze:
            %s
            
            Respond with ONLY valid JSON. Ensure all numeric fields are numbers, not arrays.
            """, 
            chunkNumber, 
            totalChunks, 
            language != null ? language : "unknown",
            code
        );
    }
    
    private void updateAnalysisStatus(String analysisId, String status, String message, Map<String, Object> result, Map<String, Object> metadata) {
        try {
            // Preserve existing metadata when updating
            if (result != null && status.equals("COMPLETED")) {
                try {
                    // Get existing item to preserve metadata
                    Item existingItem = analysisTable.getItem("analysisId", analysisId);
                    if (existingItem != null && existingItem.hasAttribute("resultJson")) {
                        String existingResultJson = existingItem.getString("resultJson");
                        Map<String, Object> existingResult = objectMapper.readValue(existingResultJson, Map.class);
                        
                        // If existing result has metadata and new result doesn't, preserve it
                        if (existingResult.containsKey("metadata") && !result.containsKey("metadata")) {
                            result.put("metadata", existingResult.get("metadata"));
                        }
                    }
                } catch (Exception e) {
                    System.err.println("Could not preserve existing metadata: " + e.getMessage());
                }
            }
            
            Item item = new Item()
                .withPrimaryKey("analysisId", analysisId)
                .withString("status", status)
                .withString("message", message != null ? message : "Status updated")
                .withLong("timestamp", System.currentTimeMillis())
                .withLong("ttl", System.currentTimeMillis() / 1000 + TimeUnit.DAYS.toSeconds(7));
            
            if (result != null) {
                try {
                    // Store as resultJson to match what the application expects
                    String resultJson = objectMapper.writeValueAsString(result);
                    item.withString("resultJson", resultJson);
                    
                    // Also store as 'result' for backward compatibility
                    item.withJSON("result", resultJson);
                } catch (Exception e) {
                    System.err.println("Failed to serialize result: " + e.getMessage());
                    // Store error info instead
                    Map<String, Object> errorResult = new HashMap<>();
                    errorResult.put("error", "Failed to serialize analysis result");
                    errorResult.put("message", e.getMessage());
                    item.withJSON("result", objectMapper.writeValueAsString(errorResult));
                }
            }
            
            analysisTable.putItem(item);
            System.out.println("Updated DynamoDB - Analysis ID: " + analysisId + ", Status: " + status);
            
        } catch (Exception e) {
            System.err.println("Failed to update DynamoDB: " + e.getMessage());
            e.printStackTrace();
            // Don't throw - we don't want DynamoDB failures to break the whole process
        }
    }
}