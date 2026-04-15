from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class ToolDef:
    name: str
    description: str
    parameters: dict[str, Any]
    fn: Callable[..., str]


TOOL_REGISTRY: dict[str, ToolDef] = {}


def register_tool(name: str, description: str, parameters: dict[str, Any]):
    """Decorator that registers a function as an LLM-callable tool."""

    def decorator(fn: Callable[..., str]) -> Callable[..., str]:
        TOOL_REGISTRY[name] = ToolDef(
            name=name,
            description=description,
            parameters=parameters,
            fn=fn,
        )
        return fn

    return decorator


def get_ollama_tools() -> list[dict[str, Any]]:
    """Return tool definitions in the format Ollama expects."""
    return [
        {
            "type": "function",
            "function": {
                "name": t.name,
                "description": t.description,
                "parameters": t.parameters,
            },
        }
        for t in TOOL_REGISTRY.values()
    ]


def execute_tool(name: str, arguments: dict[str, Any]) -> str:
    """Look up and execute a registered tool by name."""
    if name not in TOOL_REGISTRY:
        raise ValueError(f"Unknown tool: {name}")
    return TOOL_REGISTRY[name].fn(**arguments)
