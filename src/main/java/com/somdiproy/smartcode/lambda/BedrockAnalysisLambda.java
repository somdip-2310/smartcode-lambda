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
        this.analysisTable = dynamoDB.getTable(TABLE_NAME);
        
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
                updateAnalysisStatus(message.getMessageAttributes().get("analysisId").getStringValue(), 
                                   "FAILED", e.getMessage(), null);
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
        
        // Update status to PROCESSING
        updateAnalysisStatus(analysisId, "PROCESSING", "Analysis in progress", null);
        
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
            processInChunks(analysisId, code, language, context);
        } else {
            processSingleAnalysis(analysisId, code, language, context);
        }
    }
    
    private void processSingleAnalysis(String analysisId, String code, String language, Context context) throws Exception {
        context.getLogger().log("Processing single analysis for " + analysisId);
        
        String prompt = buildAnalysisPrompt(code, language);
        String result = invokeBedrockWithRetry(prompt, context);
        
        // Parse and store result
        Map<String, Object> analysisResult = objectMapper.readValue(result, Map.class);
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
            
            Map<String, Object> chunkResult = objectMapper.readValue(result, Map.class);
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
                        .modelId(MODEL_ID)
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
            
            Respond with valid JSON only.
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
            """, chunkNumber, totalChunks, language, language, code);
    }
    
    private Map<String, Object> mergeChunkResults(List<Map<String, Object>> chunkResults) {
        Map<String, Object> merged = new HashMap<>();
        
        // Aggregate all issues and suggestions
        List<Map<String, Object>> allIssues = new ArrayList<>();
        List<Map<String, Object>> allSuggestions = new ArrayList<>();
        double totalScore = 0;
        
        for (Map<String, Object> chunk : chunkResults) {
            if (chunk.containsKey("issues")) {
                allIssues.addAll((List<Map<String, Object>>) chunk.get("issues"));
            }
            if (chunk.containsKey("suggestions")) {
                allSuggestions.addAll((List<Map<String, Object>>) chunk.get("suggestions"));
            }
            if (chunk.containsKey("overallScore")) {
                totalScore += ((Number) chunk.get("overallScore")).doubleValue();
            }
        }
        
        merged.put("summary", "Comprehensive analysis completed across " + chunkResults.size() + " code segments");
        merged.put("overallScore", totalScore / chunkResults.size());
        merged.put("issues", allIssues);
        merged.put("suggestions", allSuggestions);
        merged.put("chunkCount", chunkResults.size());
        
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
                item.withJSON("result", objectMapper.writeValueAsString(result));
            }
            
            analysisTable.putItem(item);
            
        } catch (Exception e) {
            // Log error but don't fail the Lambda
            System.err.println("Failed to update DynamoDB: " + e.getMessage());
        }
    }
}