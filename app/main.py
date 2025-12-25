from fastapi import FastAPI
from pydantic import BaseModel

app =  FastAPI()

# Defines the request schema
class PromptRequest(BaseModel):
    prompt: str

# Define the response schema
class PromptResponse(BaseModel):
    received_output: str
    status: str

# Define the route
@app.post("/route",response_model= PromptResponse)

# Define handler function
def route_prompt(request: PromptRequest):
    return PromptResponse(
        received_output = request.prompt,
        status = "Router Alive"
    )
@app.get("/")
def health_check():
    return {"status": "ok"}
