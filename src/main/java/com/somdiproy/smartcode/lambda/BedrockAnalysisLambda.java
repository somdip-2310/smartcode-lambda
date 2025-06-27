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
        this.bedrockClient = BedrockRuntimeClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        
        this.dynamoDBClient = AmazonDynamoDBClientBuilder.standard().build();
        this.dynamoDB = new DynamoDB(dynamoDBClient);
        this.analysisTable = dynamoDB.getTable(TABLE_NAME != null ? TABLE_NAME : "code-analysis-results");
        
        this.s3Client = AmazonS3ClientBuilder.standard().build();
        this.objectMapper = new ObjectMapper();
    }
    
    @Override
    public Void handleRequest(SQSEvent event, Context context) {
        context.getLogger().log("Processing " + event.getRecords().size() + " messages");
        
        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                processMessage(message, context);
            } catch (Exception e) {
                context.getLogger().log("Error processing message: " + e.getMessage());
                // Update status to FAILED in DynamoDB
                String analysisId = extractAnalysisId(message);
                if (analysisId != null) {
                    updateAnalysisStatus(analysisId, "FAILED", e.getMessage(), null);
                }
                // Rethrow to let SQS retry if configured
                throw new RuntimeException("Failed to process message", e);
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
            
            context.getLogger().log("Analysis ID: " + analysisId + ", Language: " + language + ", Code Location: " + codeLocation);
            
            // Update status to PROCESSING
            updateAnalysisStatus(analysisId, "PROCESSING", "Analysis in progress", null);
            
            // Get code content
            String code;
            if ("s3".equals(codeLocation)) {
                String s3Key = (String) messageBody.get("s3Key");
                context.getLogger().log("Fetching code from S3: " + BUCKET_NAME + "/" + s3Key);
                
                try {
                    code = s3Client.getObjectAsString(BUCKET_NAME, s3Key);
                    context.getLogger().log("Successfully retrieved code from S3, length: " + code.length());
                } catch (Exception e) {
                    context.getLogger().log("Failed to retrieve from S3, checking if code is inline: " + e.getMessage());
                    
                    // Fallback: check if code is also provided inline
                    if (messageBody.containsKey("code")) {
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
            
            // Process based on size
            if (code.length() > MAX_CHUNK_SIZE) {
                processInChunks(analysisId, code, language, context);
            } else {
                processSingleAnalysis(analysisId, code, language, context);
            }
            
        } catch (Exception e) {
            context.getLogger().log("Error processing message: " + e.getMessage());
            e.printStackTrace();
            
            // Update status to FAILED in DynamoDB
            if (analysisId != null) {
                updateAnalysisStatus(analysisId, "FAILED", "Error: " + e.getMessage(), null);
            }
            
            // Rethrow to let SQS retry if configured
            throw new RuntimeException("Failed to process message", e);
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
        
        // Merge results
        Map<String, Object> mergedResult = mergeChunkResults(chunkResults);
        updateAnalysisStatus(analysisId, "COMPLETED", "Analysis completed successfully", mergedResult);
    }
    
    private Map<String, Object> parseAnalysisResult(String result, Context context) throws Exception {
        try {
            // First try to parse as JSON
            return objectMapper.readValue(result, Map.class);
        } catch (Exception e) {
            context.getLogger().log("Failed to parse result as JSON, attempting to clean: " + e.getMessage());
            
            // Try to extract JSON from the response if it contains extra text
            String cleaned = result.trim();
            
            // Remove common markdown code blocks if present
            if (cleaned.startsWith("```json")) {
                cleaned = cleaned.substring(7);
            } else if (cleaned.startsWith("```")) {
                cleaned = cleaned.substring(3);
            }
            
            if (cleaned.endsWith("```")) {
                cleaned = cleaned.substring(0, cleaned.length() - 3);
            }
            
            cleaned = cleaned.trim();
            
            try {
                return objectMapper.readValue(cleaned, Map.class);
            } catch (Exception e2) {
                context.getLogger().log("Still failed to parse after cleaning: " + e2.getMessage());
                // Return a default structure
                Map<String, Object> defaultResult = new HashMap<>();
                defaultResult.put("summary", "Analysis completed but result parsing failed");
                defaultResult.put("overallScore", 5.0);
                defaultResult.put("issues", new ArrayList<>());
                defaultResult.put("suggestions", new ArrayList<>());
                defaultResult.put("rawResponse", result);
                return defaultResult;
            }
        }
    }
    
    private String invokeBedrockWithRetry(String prompt, Context context) throws Exception {
        int maxRetries = 3;
        int retryDelay = 2000; // Start with 2 seconds
        
        for (int attempt = 0; attempt < maxRetries; attempt++) {
            try {
                // Build request for Nova Premier
                Map<String, Object> requestBody = new HashMap<>();
                
                List<Map<String, Object>> messages = new ArrayList<>();
                Map<String, Object> message = new HashMap<>();
                message.put("role", "user");
                
                List<Map<String, Object>> contentArray = new ArrayList<>();
                Map<String, Object> textContent = new HashMap<>();
                textContent.put("text", prompt);
                contentArray.add(textContent);
                
                message.put("content", contentArray);
                messages.add(message);
                
                requestBody.put("messages", messages);
                
                Map<String, Object> inferenceConfig = new HashMap<>();
                inferenceConfig.put("maxTokens", 4000);
                inferenceConfig.put("temperature", 0.1);
                inferenceConfig.put("topP", 0.9);
                
                requestBody.put("inferenceConfig", inferenceConfig);
                
                String jsonBody = objectMapper.writeValueAsString(requestBody);
                
                InvokeModelRequest request = InvokeModelRequest.builder()
                        .modelId(MODEL_ID != null ? MODEL_ID : "amazon.nova-premier-v1:0")
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
                            return (String) content.get(0).get("text");
                        }
                    }
                }
                
                throw new RuntimeException("Invalid response from Bedrock");
                
            } catch (Exception e) {
                context.getLogger().log("Bedrock invocation failed (attempt " + (attempt + 1) + "): " + e.getMessage());
                
                if (attempt < maxRetries - 1) {
                    Thread.sleep(retryDelay);
                    retryDelay *= 2; // Exponential backoff
                } else {
                    throw e;
                }
            }
        }
        
        throw new RuntimeException("Failed to invoke Bedrock after " + maxRetries + " attempts");
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
            
            Focus on:
            1. Security vulnerabilities
            2. Performance issues
            3. Code quality and maintainability
            4. Best practices
            5. Potential bugs
            
            Provide your response in JSON format with this structure:
            {
              "summary": "Brief overview",
              "overallScore": 8.5,
              "issues": [{
                "severity": "HIGH",
                "type": "SECURITY",
                "title": "Issue title",
                "description": "Description",
                "lineNumber": 15,
                "suggestion": "How to fix"
              }],
              "suggestions": [{
                "title": "Suggestion",
                "description": "Description",
                "category": "Performance",
                "impact": "High"
              }],
              "security": {
                "securityScore": 7.5,
                "vulnerabilities": [],
                "hasSecurityIssues": false
              },
              "performance": {
                "performanceScore": 8.0,
                "bottlenecks": [],
                "complexity": "Medium"
              }
            }
            
            Code:
            ```%s
            %s
            ```
            
            Respond with ONLY valid JSON without any markdown formatting or code blocks. Do not include ``` or ```json tags.
            """, language, language, code);
    }
    
    private String buildChunkAnalysisPrompt(String code, String language, int chunkNumber, int totalChunks) {
        return String.format("""
            Analyzing chunk %d of %d of %s code.
            
            Analyze for issues and improvements in this code segment.
            Note: This is a partial analysis of a larger file.
            
            Use the same JSON response format as specified.
            
            Code chunk:
            ```%s
            %s
            ```
            
            Respond with ONLY valid JSON without any markdown formatting or code blocks.
            """, chunkNumber, totalChunks, language, language, code);
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
                .withString("message", message)
                .withLong("timestamp", System.currentTimeMillis())
                .withLong("ttl", System.currentTimeMillis() / 1000 + TimeUnit.DAYS.toSeconds(7)); // 7 days TTL
            
            if (result != null) {
                // CRITICAL FIX: Store as "resultJson" instead of "result"
                // This matches what the Spring application expects
                String resultJsonString = objectMapper.writeValueAsString(result);
                item.withString("resultJson", resultJsonString);
                
                // Also log the size for debugging
                System.out.println("Storing resultJson for " + analysisId + ", size: " + resultJsonString.length() + " bytes");
            }
            
            analysisTable.putItem(item);
            System.out.println("Successfully updated DynamoDB - Analysis ID: " + analysisId + ", Status: " + status);
            
        } catch (Exception e) {
            // Log error but don't fail the Lambda
            System.err.println("Failed to update DynamoDB: " + e.getMessage());
            e.printStackTrace();
        }
    }
}