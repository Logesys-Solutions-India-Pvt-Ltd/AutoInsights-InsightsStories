from flask import Flask, request, jsonify, Response
from initializer import main_handler

app = Flask(__name__)

@app.route("/trigger", methods=["POST"])
def trigger_metadata():
    try:
        event = request.get_json()
        result = main_handler(event)  # Call your handler

        return Response(result, status=200, mimetype='application/json')
        # OR if it's a dict: return jsonify(result), 200

    except Exception as e:
        return jsonify({"status": "ERROR", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
