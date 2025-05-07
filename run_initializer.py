#!/usr/bin/env python3
from flask import Flask, request, jsonify, Response
from initializer import main_handler

app = Flask(__name__)

@app.route("/trigger", methods=["POST"])
def trigger_metadata():
    try:
        event = request.get_json()
        result = main_handler(event)

        if isinstance(result, dict): 
            if result.get("status") == "error":  
                return jsonify({"status": "ERROR", "message": result["message"]}), 500
            elif result.get("status") == "success":
                return jsonify(result), 200 
            else:
                return jsonify(result), 200
        elif isinstance(result, str):
             return Response(result, status=200, mimetype='application/json')
        else:
            return jsonify({"status": "SUCCESS", "message": "Processed successfully", "data": result}), 200


    except Exception as e:
        error_message = f"Error in trigger_metadata: {e}"
        print(error_message)  
        return jsonify({"status": "ERROR", "message": error_message}), 500
    
# Comment out or remove for production with Gunicorn
# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000)