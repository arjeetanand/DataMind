import os
import json
from typing import List

from langchain_oci import ChatOCIGenAI, encode_image, is_vision_model, load_image
from langchain_core.tools import StructuredTool
from langchain_core.messages import AIMessage, ToolMessage, HumanMessage, SystemMessage, AnyMessage
from .schema import LLMRequest, LLMResponse
import base64

class OCILLMService:
    def __init__(self):
        self.llm = ChatOCIGenAI(
            model_id=os.getenv("OCI_GENAI_MODEL_ID"),
            compartment_id=os.getenv("OCI_GENAI_COMPARTMENT_ID"),
            service_endpoint=os.getenv("OCI_GENAI_SERVICE_ENDPOINT"),
            auth_type=os.getenv("OCI_AUTH_TYPE"),
            auth_profile=os.getenv("OCI_CONFIG_PROFILE"),
            auth_file_location=os.getenv("OCI_CONFIG_FILE"),
            model_kwargs={
                "temperature": float(os.getenv("LLM_TEMPERATURE", 0.7)),
                # "max_tokens": int(os.getenv("LLM_MAX_TOKENS", 500)),
                # "top_p": float(os.getenv("LLM_TOP_P", 0.95)),
                # "top_k": int(os.getenv("LLM_TOP_K", 50))
            }
        )

    async def invoke(self, request: LLMRequest) -> AIMessage:
        """Unified invoke supporting single-turn and multi-turn via LLMRequest.

        - If request.messages is provided, it is used directly.
        - Otherwise, a single-turn message list is built from query + prompt.
        - Supports optional images and tool binding.
        """
        # Override model_id if provided
        model_id = request.model_id or self.llm.model_id
        if model_id != self.llm.model_id:
            self.llm = self.llm.with_config(model_id=model_id)

        if request.messages:
            prepared_messages = list(request.messages)
        else:
            prepared_messages = []
            if request.prompt_template:
                prepared_messages.append(SystemMessage(content=request.prompt_template))

            user_content = [{"type": "text", "text": request.query or ""}]
            if request.images:
                if not is_vision_model(model_id=self.llm.model_id):
                    raise ValueError(f"{self.llm.model_id} does not support vision inputs")
                for img_base64 in request.images:
                    user_content.append(encode_image(base64.b64decode(img_base64), mime_type="image/png"))

            prepared_messages.append(HumanMessage(content=user_content))

        # Ensure system prompt is present if given
        if request.prompt_template:
            has_system = any(isinstance(m, SystemMessage) for m in prepared_messages)
            if not has_system:
                prepared_messages = [SystemMessage(content=request.prompt_template)] + prepared_messages

        # Bind tools
        if request.tools:
            choice = os.getenv("LLM_TOOL_CHOICE", "auto").strip().lower()
            if choice in {"required", "any", "force"}:
                llm_with_tools = self.llm.bind_tools(request.tools, tool_choice="required")
            elif choice in {"none", "disabled", "off"}:
                llm_with_tools = self.llm
            else:
                llm_with_tools = self.llm.bind_tools(request.tools)
        else:
            llm_with_tools = self.llm

        return await llm_with_tools.ainvoke(prepared_messages)
