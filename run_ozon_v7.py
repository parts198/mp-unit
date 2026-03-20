import uvicorn
import ozon_backend_v7 as appmod

if __name__ == "__main__":
    uvicorn.run(appmod.app, host="0.0.0.0", port=8000)
