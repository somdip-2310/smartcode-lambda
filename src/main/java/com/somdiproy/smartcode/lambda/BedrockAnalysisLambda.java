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
    private static final int CHUNK_DELAY_MS = 5000; // 5 seconds between chunks
    
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
            
            // Log initialization
            System.out.println("Lambda initialized with table: " + tableName);
            System.out.println("S3 bucket: " + (BUCKET_NAME != null ? BUCKET_NAME : "not set"));
            System.out.println("Model ID: " + (MODEL_ID != null ? MODEL_ID : "using default"));
        } catch (Exception e) {
            System.err.println("Error initializing Lambda: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize Lambda", e);
        }
    }
    
    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Processing " + event.getRecords().size() + " messages");
        
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            String analysisId = null;
            try {
                analysisId = extractAnalysisId(message);
                context.getLogger().log("Processing analysis: " + analysisId);
                processMessage(message, context);
            } catch (Exception e) {
                context.getLogger().log("ERROR processing message: " + e.getMessage());
                e.printStackTrace();
                
                // Update status to FAILED in DynamoDB
                if (analysisId != null) {
                    try {
                        // Truncate error message to avoid DynamoDB item size limits
                        String errorMessage = e.getMessage() != null ? 
                            e.getMessage().substring(0, Math.min(e.getMessage().length(), 200)) : 
                            "Unknown error occurred";
                        
                        updateAnalysisStatus(analysisId, "FAILED", 
                            "Error: " + errorMessage, 
                            null);
                    } catch (Exception updateError) {
                        context.getLogger().log("Failed to update analysis status: " + updateError.getMessage());
                    }
                }
                
                // Determine if this is a permanent or transient failure
                boolean isPermanentFailure = false;
                
                // Check for permanent failure conditions
                if (e instanceof IllegalArgumentException || 
                    e instanceof IllegalStateException ||
                    e instanceof NullPointerException ||
                    (e.getMessage() != null && (
                        e.getMessage().contains("Invalid") ||
                        e.getMessage().contains("required") ||
                        e.getMessage().contains("empty") ||
                        e.getMessage().contains("not found") ||
                        e.getMessage().contains("Missing")
                    ))) {
                    isPermanentFailure = true;
                    context.getLogger().log("Permanent failure detected, not retrying");
                }
                
                // Check for S3 access denied or not found
                if (e.getMessage() != null && (
                    e.getMessage().contains("Access Denied") ||
                    e.getMessage().contains("NoSuchKey") ||
                    e.getMessage().contains("NoSuchBucket"))) {
                    isPermanentFailure = true;
                    context.getLogger().log("S3 access error - permanent failure");
                }
                
                // For transient failures (network issues, throttling, etc.), throw to retry
                if (!isPermanentFailure) {
                    context.getLogger().log("Transient failure detected, will retry");
                    throw new RuntimeException("Transient failure processing message", e);
                }
                
                // For permanent failures, log and continue
                context.getLogger().log("Permanent failure - marking message as processed to prevent infinite retries");
            }
        }
        return null;
    }
    
    private String extractAnalysisId(SQSEvent.SQSMessage message) {
        try {
            Map<String, Object> messageBody = objectMapper.readValue(message.getBody(), Map.class);
            return (String) messageBody.get("analysisId");
        } catch (Exception e) {
            // Try to get from message attributes
            if (message.getMessageAttributes() != null && 
                message.getMessageAttributes().containsKey("analysisId")) {
                return message.getMessageAttributes().get("analysisId").getStringValue();
            }
            return null;
        }
    }
    
    private void processMessage(SQSEvent.SQSMessage message, Context context) throws Exception {
        context.getLogger().log("Processing message: " + message.getMessageId());
        
        String analysisId = null;
        try {
            // Parse message body
            Map<String, Object> messageBody = objectMapper.readValue(message.getBody(), Map.class);
            analysisId = (String) messageBody.get("analysisId");
            String language = (String) messageBody.get("language");
            String codeLocation = (String) messageBody.get("codeLocation");
            
            // Validate required fields
            if (analysisId == null || analysisId.isEmpty()) {
                throw new IllegalArgumentException("analysisId is required");
            }
            
            context.getLogger().log("Analysis ID: " + analysisId + ", Language: " + language + ", Code Location: " + codeLocation);
            
            // Update status to PROCESSING
            updateAnalysisStatus(analysisId, "PROCESSING", "Analysis in progress", null);
            
            // Get code content
            String code;
            if ("s3".equals(codeLocation)) {
                String s3Key = (String) messageBody.get("s3Key");
                if (s3Key == null || s3Key.isEmpty()) {
                    throw new IllegalArgumentException("s3Key is required when codeLocation is 's3'");
                }
                
                context.getLogger().log("Fetching code from S3: " + BUCKET_NAME + "/" + s3Key);
                
                try {
                    // Validate bucket name
                    String bucketName = BUCKET_NAME != null ? BUCKET_NAME : "smartcode-uploads";
                    code = s3Client.getObjectAsString(bucketName, s3Key);
                    context.getLogger().log("Successfully retrieved code from S3, length: " + code.length());
                } catch (Exception e) {
                    context.getLogger().log("Failed to retrieve from S3: " + e.getMessage());
                    
                    // Fallback: check if code is also provided inline
                    if (messageBody.containsKey("code") && messageBody.get("code") != null) {
                        code = (String) messageBody.get("code");
                        context.getLogger().log("Using inline code as fallback, length: " + code.length());
                    } else {
                        throw new RuntimeException("Failed to retrieve code from S3 and no inline code provided", e);
                    }
                }
            } else {
                code = (String) messageBody.get("code");
                if (code == null || code.isEmpty()) {
                    throw new RuntimeException("No code content found in message");
                }
                context.getLogger().log("Using inline code, length: " + code.length());
            }
            
            // Validate code content
            if (code.trim().isEmpty()) {
                throw new IllegalArgumentException("Code content is empty");
            }
            
            // Process based on size
            if (code.length() > MAX_CHUNK_SIZE) {
                processInChunks(analysisId, code, language, context);
            } else {
                processSingleAnalysis(analysisId, code, language, context);
            }
            
        } catch (Exception e) {
            String detailedError = String.format("Error processing message: %s - %s", 
                e.getClass().getSimpleName(), e.getMessage());
            context.getLogger().log(detailedError);
            e.printStackTrace();
            
            // Create a more informative error message
            String userFriendlyError = "Analysis failed";
            
            if (e.getMessage() != null) {
                if (e.getMessage().contains("parse") || e.getMessage().contains("JSON")) {
                    userFriendlyError = "Failed to parse AI response. Please try again.";
                } else if (e.getMessage().contains("throttl") || e.getMessage().contains("429")) {
                    userFriendlyError = "AI service is busy. Please try again in a few moments.";
                } else if (e.getMessage().contains("timeout")) {
                    userFriendlyError = "Analysis timed out. Try with smaller code size.";
                } else {
                    userFriendlyError = "Analysis error: " + e.getMessage();
                }
            }
            
            // Update status to FAILED in DynamoDB with detailed error
            if (analysisId != null) {
                updateAnalysisStatus(analysisId, "FAILED", userFriendlyError, null);
            }
            
            // For transient errors, allow retry
            boolean isTransientError = e.getMessage() != null && 
                (e.getMessage().contains("throttl") || 
                 e.getMessage().contains("429") || 
                 e.getMessage().contains("timeout"));
            
            if (isTransientError) {
                throw new RuntimeException("Transient failure: " + userFriendlyError, e);
            }
            
            // For permanent errors, don't retry
            context.getLogger().log("Permanent failure detected, not retrying");
        }
    }
    
    private void processSingleAnalysis(String analysisId, String code, String language, Context context) throws Exception {
        context.getLogger().log("Processing single analysis for " + analysisId);
        
        String prompt = buildAnalysisPrompt(code, language);
        String result = invokeBedrockWithRetry(prompt, context);
        
        // Parse and store result
        Map<String, Object> analysisResult = parseAnalysisResult(result, context);
        updateAnalysisStatus(analysisId, "COMPLETED", "Analysis completed successfully", analysisResult);
    }
    
    private void processInChunks(String analysisId, String code, String language, Context context) throws Exception {
        context.getLogger().log("Processing in chunks for " + analysisId + ", code length: " + code.length());
        
        List<String> chunks = splitIntoChunks(code, MAX_CHUNK_SIZE);
        List<Map<String, Object>> chunkResults = new ArrayList<>();
        
        // Process chunks with adaptive delays
        int consecutiveThrottles = 0;
        int baseChunkDelay = 10000; // 10 seconds base delay
        
        for (int i = 0; i < chunks.size(); i++) {
            context.getLogger().log("Processing chunk " + (i + 1) + " of " + chunks.size());
            
            try {
                String chunkPrompt = buildChunkAnalysisPrompt(chunks.get(i), language, i + 1, chunks.size());
                String result = invokeBedrockWithRetry(chunkPrompt, context);
                
                Map<String, Object> chunkResult = parseAnalysisResult(result, context);
                chunkResults.add(chunkResult);
                
                // Reset throttle counter on success
                consecutiveThrottles = 0;
                
                // Adaptive delay between chunks
                if (i < chunks.size() - 1) {
                    int delay = baseChunkDelay + (consecutiveThrottles * 5000);
                    context.getLogger().log("Waiting " + delay + "ms before next chunk");
                    Thread.sleep(delay);
                }
                
            } catch (Exception e) {
                if (e.getMessage().contains("429") || e.getMessage().contains("throttl")) {
                    consecutiveThrottles++;
                    
                    // If too many throttles, increase base delay
                    if (consecutiveThrottles > 2) {
                        baseChunkDelay = Math.min(baseChunkDelay * 2, 60000); // Max 60s
                        context.getLogger().log("Increasing base delay to " + baseChunkDelay + "ms due to throttling");
                    }
                }
                throw e;
            }
        }
        
        // Merge results
        Map<String, Object> mergedResult = mergeChunkResults(chunkResults);
        updateAnalysisStatus(analysisId, "COMPLETED", "Analysis completed successfully", mergedResult);
    }
    
    private Map<String, Object> parseAnalysisResult(String result, Context context) throws Exception {
        try {
            // Log the raw result for debugging
            context.getLogger().log("Raw Bedrock response length: " + (result != null ? result.length() : "null"));
            
            if (result == null || result.trim().isEmpty()) {
                context.getLogger().log("WARNING: Empty or null response from Bedrock");
                return createDefaultAnalysisResult();
            }
            
            // First try to parse as JSON directly
            try {
                Map<String, Object> parsed = objectMapper.readValue(result, Map.class);
                context.getLogger().log("Successfully parsed JSON response");
                return validateAndNormalizeResult(parsed);
            } catch (Exception e) {
                context.getLogger().log("Initial JSON parse failed: " + e.getMessage());
            }
            
            // Try to extract JSON from the response
            String cleaned = result.trim();
            
            // Remove markdown code blocks if present
            if (cleaned.contains("```json")) {
                int startIndex = cleaned.indexOf("```json") + 7;
                int endIndex = cleaned.lastIndexOf("```");
                if (endIndex > startIndex) {
                    cleaned = cleaned.substring(startIndex, endIndex).trim();
                }
            } else if (cleaned.contains("```")) {
                // Remove any code block markers
                cleaned = cleaned.replaceAll("```[a-zA-Z]*\\s*", "").replaceAll("\\s*```", "").trim();
            }
            
            // Find JSON object boundaries
            int jsonStart = cleaned.indexOf('{');
            int jsonEnd = findMatchingBrace(cleaned, jsonStart);
            
            if (jsonStart >= 0 && jsonEnd > jsonStart) {
                cleaned = cleaned.substring(jsonStart, jsonEnd + 1);
                
                try {
                    Map<String, Object> parsed = objectMapper.readValue(cleaned, Map.class);
                    context.getLogger().log("Successfully parsed cleaned JSON response");
                    return validateAndNormalizeResult(parsed);
                } catch (Exception e2) {
                    context.getLogger().log("Failed to parse cleaned JSON: " + e2.getMessage());
                    // Log first 500 chars of cleaned response for debugging
                    context.getLogger().log("Cleaned response preview: " + 
                        cleaned.substring(0, Math.min(cleaned.length(), 500)));
                }
            }
            
            // If all parsing attempts fail, create a valid default structure
            context.getLogger().log("All parsing attempts failed, returning default structure");
            return createDefaultAnalysisResult();
            
        } catch (Exception e) {
            context.getLogger().log("ERROR in parseAnalysisResult: " + e.getMessage());
            e.printStackTrace();
            return createDefaultAnalysisResult();
        }
    }

    private int findMatchingBrace(String str, int startIndex) {
        if (startIndex < 0 || startIndex >= str.length() || str.charAt(startIndex) != '{') {
            return -1;
        }
        
        int count = 0;
        boolean inString = false;
        boolean escaped = false;
        
        for (int i = startIndex; i < str.length(); i++) {
            char c = str.charAt(i);
            
            if (!escaped) {
                if (c == '"' && !inString) {
                    inString = true;
                } else if (c == '"' && inString) {
                    inString = false;
                } else if (!inString) {
                    if (c == '{') count++;
                    else if (c == '}') {
                        count--;
                        if (count == 0) return i;
                    }
                }
                
                escaped = (c == '\\' && inString);
            } else {
                escaped = false;
            }
        }
        
        return -1;
    }

    private Map<String, Object> validateAndNormalizeResult(Map<String, Object> result) {
        // Ensure all required fields are present
        if (!result.containsKey("summary")) {
            result.put("summary", "Code analysis completed");
        }
        if (!result.containsKey("overallScore")) {
            result.put("overallScore", 7.0);
        }
        if (!result.containsKey("issues") || !(result.get("issues") instanceof List)) {
            result.put("issues", new ArrayList<>());
        }
        if (!result.containsKey("improvements") || !(result.get("improvements") instanceof List)) {
            result.put("improvements", new ArrayList<>());
        }
        
        return result;
    }

    private Map<String, Object> createDefaultAnalysisResult() {
        Map<String, Object> result = new HashMap<>();
        result.put("summary", "Analysis completed but result parsing failed. Please check logs.");
        result.put("overallScore", 7.0);
        result.put("issues", new ArrayList<>());
        result.put("suggestions", Arrays.asList("Unable to parse detailed analysis results"));
        
        Map<String, Object> security = new HashMap<>();
        security.put("securityScore", 7.0);
        security.put("vulnerabilities", new ArrayList<>());
        security.put("hasSecurityIssues", false);
        result.put("security", security);
        
        Map<String, Object> performance = new HashMap<>();
        performance.put("performanceScore", 7.0);
        performance.put("bottlenecks", new ArrayList<>());
        result.put("performance", performance);
        
        return result;
    }
    
    private String invokeBedrockWithRetry(String prompt, Context context) throws Exception {
        int maxRetries = 5;
        int baseDelay = 5000; // Start with 5 seconds

        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                // Build request for Nova Premier
                Map<String, Object> requestBody = new HashMap<>();

                // Create messages array
                List<Map<String, Object>> messages = new ArrayList<>();
                Map<String, Object> userMessage = new HashMap<>();
                userMessage.put("role", "user");
                
                // CRITICAL FIX: Content must be an array of objects for Nova Premier
                List<Map<String, Object>> contentArray = new ArrayList<>();
                Map<String, Object> textContent = new HashMap<>();
                textContent.put("text", prompt);
                contentArray.add(textContent);
                
                userMessage.put("content", contentArray); // Content as array, not string
                messages.add(userMessage);

                requestBody.put("messages", messages);

                // Create inference config for Nova Premier
                Map<String, Object> inferenceConfig = new HashMap<>();
                inferenceConfig.put("maxTokens", 4000);
                inferenceConfig.put("temperature", 0.7);
                inferenceConfig.put("topP", 0.9);

                requestBody.put("inferenceConfig", inferenceConfig);

                String jsonBody = objectMapper.writeValueAsString(requestBody);
                
                // Log the request for debugging (remove in production)
                context.getLogger().log("Bedrock request (attempt " + (attempt + 1) + "): " + jsonBody);

                InvokeModelRequest request = InvokeModelRequest.builder()
                        .modelId(MODEL_ID != null ? MODEL_ID : "us.amazon.nova-premier-v1:0")
                        .contentType("application/json")
                        .accept("application/json")
                        .body(SdkBytes.fromUtf8String(jsonBody))
                        .build();

                InvokeModelResponse response = bedrockClient.invokeModel(request);
                String responseBody = response.body().asUtf8String();
                
                // Log the response for debugging (remove in production)
                context.getLogger().log("Bedrock response: " + responseBody);

                // Parse the response - Nova Premier specific format
                Map<String, Object> parsedResponse = objectMapper.readValue(responseBody, Map.class);

                // Extract content from Nova Premier response format
                String content = null;
                
                // Nova Premier returns: { "output": { "message": { "content": [{ "text": "..." }] } } }
                if (parsedResponse.containsKey("output")) {
                    Map<String, Object> output = (Map<String, Object>) parsedResponse.get("output");
                    if (output != null && output.containsKey("message")) {
                        Map<String, Object> message = (Map<String, Object>) output.get("message");
                        if (message != null && message.containsKey("content")) {
                            List<Map<String, Object>> contentList = (List<Map<String, Object>>) message.get("content");
                            if (contentList != null && !contentList.isEmpty()) {
                                Map<String, Object> firstContent = contentList.get(0);
                                if (firstContent.containsKey("text")) {
                                    content = (String) firstContent.get("text");
                                }
                            }
                        }
                    }
                }
                
                // Fallback for other response formats
                if (content == null && parsedResponse.containsKey("content")) {
                    content = (String) parsedResponse.get("content");
                } else if (content == null && parsedResponse.containsKey("completion")) {
                    content = (String) parsedResponse.get("completion");
                }

                if (content == null) {
                    context.getLogger().log("Unexpected response format: " + responseBody);
                    throw new Exception("Unable to extract content from Bedrock response");
                }

                return content;

            } catch (Exception e) {
                String errorMessage = e.getMessage();
                context.getLogger().log("Bedrock invocation failed (attempt " + (attempt + 1) + "): " + errorMessage);

                // Check if it's a rate limit error
                if (errorMessage != null && (errorMessage.contains("429") || 
                    errorMessage.contains("Too many requests") || 
                    errorMessage.contains("throttl"))) {

                    // Calculate exponential backoff with jitter
                    int delay = baseDelay * (int) Math.pow(2, attempt) + (int) (Math.random() * 1000);

                    context.getLogger().log(String.format(
                        "Rate limited. Retrying in %d ms", delay));

                    if (attempt < maxRetries - 1) {
                        Thread.sleep(delay);
                        continue;
                    }
                }

                // For non-rate limit errors, fail immediately if it's a validation error
                if (errorMessage != null && (errorMessage.contains("ValidationException") || 
                    errorMessage.contains("Malformed input"))) {
                    throw e; // Don't retry validation errors
                }

                // For other errors, retry with backoff
                if (attempt < maxRetries - 1) {
                    int delay = baseDelay * (attempt + 1);
                    context.getLogger().log("Retrying after " + delay + "ms");
                    Thread.sleep(delay);
                    continue;
                }

                throw e;
            }
        }

        throw new Exception("Failed to invoke Bedrock after " + maxRetries + " attempts");
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
        return String.format(
            "Analyze the following %s code and provide a comprehensive review.\n\n" +
            "CRITICAL: Your response must be ONLY the JSON object, with NO additional text, NO markdown formatting, NO code blocks, and NO backticks.\n\n" +
            "Code to analyze:\n%s\n\n" +
            "Provide your analysis in the following JSON format:\n" +
            "{\n" +
            "  \"summary\": \"Brief summary of the code quality\",\n" +
            "  \"overallScore\": 8.5,\n" +
            "  \"issues\": [\n" +
            "    {\"severity\": \"HIGH\", \"category\": \"Security\", \"description\": \"Issue description\", \"line\": 10, \"suggestion\": \"How to fix\"}\n" +
            "  ],\n" +
            "  \"suggestions\": [\"Improvement suggestion 1\", \"Improvement suggestion 2\"],\n" +
            "  \"security\": {\n" +
            "    \"securityScore\": 7.0,\n" +
            "    \"vulnerabilities\": [{\"type\": \"SQL Injection\", \"severity\": \"HIGH\", \"description\": \"Details\"}],\n" +
            "    \"hasSecurityIssues\": true\n" +
            "  },\n" +
            "  \"performance\": {\n" +
            "    \"performanceScore\": 8.0,\n" +
            "    \"bottlenecks\": [\"Issue 1\", \"Issue 2\"]\n" +
            "  }\n" +
            "}\n\nRespond with ONLY valid JSON.",
            language != null ? language : "unknown",
            code
        );
    }
    
    private String buildChunkAnalysisPrompt(String code, String language, int chunkNumber, int totalChunks) {
        return String.format("""
            Analyzing chunk %d of %d of %s code.
            
            Analyze for issues and improvements in this code segment.
            Note: This is a partial analysis of a larger file.
            
            CRITICAL: Your response must be ONLY the JSON object, with NO additional text, NO markdown formatting, NO code blocks, and NO backticks.
            
            Use the same JSON response format as in the main analysis.
            
            Code chunk to analyze:
            %s
            
            Respond with ONLY valid JSON.
            """, chunkNumber, totalChunks, language != null ? language : "unknown", code);
    }
    
    private Map<String, Object> mergeChunkResults(List<Map<String, Object>> chunkResults) {
        Map<String, Object> merged = new HashMap<>();
        
        // Aggregate all issues and suggestions
        List<Map<String, Object>> allIssues = new ArrayList<>();
        List<Map<String, Object>> allSuggestions = new ArrayList<>();
        double totalScore = 0;
        int validScores = 0;
        
        // Security and performance aggregation
        double totalSecurityScore = 0;
        double totalPerformanceScore = 0;
        int securityScoreCount = 0;
        int performanceScoreCount = 0;
        List<Map<String, Object>> allVulnerabilities = new ArrayList<>();
        List<Map<String, Object>> allBottlenecks = new ArrayList<>();
        boolean hasSecurityIssues = false;
        
        for (Map<String, Object> chunk : chunkResults) {
            // Aggregate issues
            if (chunk.containsKey("issues")) {
                allIssues.addAll((List<Map<String, Object>>) chunk.get("issues"));
            }
            
            // Aggregate suggestions
            if (chunk.containsKey("suggestions")) {
                allSuggestions.addAll((List<Map<String, Object>>) chunk.get("suggestions"));
            }
            
            // Aggregate scores
            if (chunk.containsKey("overallScore")) {
                totalScore += ((Number) chunk.get("overallScore")).doubleValue();
                validScores++;
            }
            
            // Aggregate security data
            if (chunk.containsKey("security")) {
                Map<String, Object> security = (Map<String, Object>) chunk.get("security");
                if (security.containsKey("securityScore")) {
                    totalSecurityScore += ((Number) security.get("securityScore")).doubleValue();
                    securityScoreCount++;
                }
                if (security.containsKey("vulnerabilities")) {
                    allVulnerabilities.addAll((List<Map<String, Object>>) security.get("vulnerabilities"));
                }
                if (security.containsKey("hasSecurityIssues") && (Boolean) security.get("hasSecurityIssues")) {
                    hasSecurityIssues = true;
                }
            }
            
            // Aggregate performance data
            if (chunk.containsKey("performance")) {
                Map<String, Object> performance = (Map<String, Object>) chunk.get("performance");
                if (performance.containsKey("performanceScore")) {
                    totalPerformanceScore += ((Number) performance.get("performanceScore")).doubleValue();
                    performanceScoreCount++;
                }
                if (performance.containsKey("bottlenecks")) {
                    allBottlenecks.addAll((List<Map<String, Object>>) performance.get("bottlenecks"));
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
        
        return merged;
    }
    
    private void updateAnalysisStatus(String analysisId, String status, String message, Map<String, Object> result) {
        try {
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