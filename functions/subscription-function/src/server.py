from dotenv import load_dotenv
load_dotenv('../.env')

from app.views import app

if __name__ == '__main__':
    app.run(port=3000, debug=True)