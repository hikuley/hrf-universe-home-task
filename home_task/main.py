from fastapi import FastAPI, Depends, HTTPException

app = FastAPI()


@app.get("/hello")
def read_hello():
    return {"message": "Hello World"}
