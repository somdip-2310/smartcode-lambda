package com.somdiproy.smartcode.dto;

import java.util.List;
import java.util.Map;

public class CodeReviewResult {
    private String summary;
    private Double overallScore;
    private List<String> issues;
    private List<String> suggestions;
    private Map<String, Object> security;
    private Map<String, Object> performance;
    private Map<String, Object> quality;
    private Map<String, Object> metadata;
    
    // Generate getters and setters
    // Right-click → Source → Generate Getters and Setters → Select All → Generate
    
    public String getSummary() { return summary; }
    public void setSummary(String summary) { this.summary = summary; }
    
    public Double getOverallScore() { return overallScore; }
    public void setOverallScore(Double overallScore) { this.overallScore = overallScore; }
    
    public List<String> getIssues() { return issues; }
    public void setIssues(List<String> issues) { this.issues = issues; }
    
    public List<String> getSuggestions() { return suggestions; }
    public void setSuggestions(List<String> suggestions) { this.suggestions = suggestions; }
    
    public Map<String, Object> getSecurity() { return security; }
    public void setSecurity(Map<String, Object> security) { this.security = security; }
    
    public Map<String, Object> getPerformance() { return performance; }
    public void setPerformance(Map<String, Object> performance) { this.performance = performance; }
    
    public Map<String, Object> getQuality() { return quality; }
    public void setQuality(Map<String, Object> quality) { this.quality = quality; }
    
    public Map<String, Object> getMetadata() { return metadata; }
    public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }
}