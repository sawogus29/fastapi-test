# app3.py
from fastapi import FastAPI, Request
import logging
from starlette.middleware.base import BaseHTTPMiddleware

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Middleware to log requests
class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        logger.info("Middleware 2: Before")
        response = await call_next(request)
        logger.info("Middleware 2: After")
        return response

# Add the middleware to the app
app.add_middleware(LoggingMiddleware)

# Print the middleware stack
for middleware in app.user_middleware:
    print(f"Middleware class: {middleware}")

print('-------building new middleware----------')
new_middlewares = []
otel_middleware_bak = None
for middleware in app.user_middleware:
    if middleware.cls.__name__ == 'OpenTelemetryMiddleware':
        otel_middleware_bak = middleware
    else:
        new_middlewares.append(middleware)
if otel_middleware_bak:
    new_middlewares = [otel_middleware_bak] + new_middlewares
app.user_middleware = new_middlewares
app.middleware_stack = app.build_middleware_stack()

# Print the middleware stack
print('--------new middleware---------')
for middleware in app.user_middleware:
    print(f"Middleware class: {middleware}")

@app.get("/good")
async def good_endpoint():
    logger.info(f"inside GET /good")
    return {"message": "This is the good endpoint"}

@app.post("/bad")
async def bad_endpoint(request: Request):
    logger.info(f"inside POST /bad")
    data = await request.json()
    error_info = data.get('error', 'No error info provided')
    return {"message": "This is the bad endpoint", "error": error_info}
