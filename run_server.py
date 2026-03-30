import uvicorn
import sys
import os

if getattr(sys, 'frozen', False):
    # Running as compiled .exe
    sys.path.append(os.path.join(sys._MEIPASS, "app"))
    
if __name__ == "__main__":
    uvicorn.run("app.main:app", host="0.0.0.0", port=8003)