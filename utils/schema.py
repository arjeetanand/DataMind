from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from langchain_core.tools import StructuredTool
from langchain_core.messages import AnyMessage

class LLMRequest(BaseModel):
    query: Optional[str] = Field(default=None, description="The user's natural language query")
    tools: Optional[List[StructuredTool]] = Field(default=None, description="List of tools to bind")
    prompt_template: Optional[str] = Field(default=None, description="System prompt template")
    model_id: Optional[str] = Field(default=None, description="LLM model ID")
    session_id: Optional[str] = Field(default=None, description="Session identifier")
    request_id: Optional[str] = Field(default=None, description="Unique request identifier")
    images: Optional[List[str]] = Field(default=None, description="List of base64 encoded images for vision processing")
    messages: Optional[List[AnyMessage]] = Field(default=None, description="Pre-built message list")

class LLMResponse(BaseModel):
    content: str = Field(description="The response content")
    status: str = Field(default="success", description="Response status")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional metadata")
    tool_calls: Optional[List[Dict[str, Any]]] = Field(default=None, description="Tool calls emitted by the model")