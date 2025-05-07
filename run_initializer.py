#!/usr/bin/env python3
import os
import signal
from flask import Flask, request, jsonify, Response
from initializer import main_handler

app = Flask(__name__)

# @app.route("/test", methods=["GET"])
# def hello():
#     return "Hello world"

def stop_gunicorn():
    """
    Stops the Gunicorn master process.  This should be called after the
    application has finished processing its task and sent the response.
    """
    server_pid = os.getppid()  # Get the parent process ID (Gunicorn master)
    os.kill(server_pid, signal.SIGTERM)  # Send a termination signal


@app.route("/trigger", methods=["POST"])
def trigger_insights_generation():
    try:
        event = request.get_json()
        result = main_handler(event)

        # return "Hello World"
        if isinstance(result, dict): 
            print(result.get())
            if result.get("status") == "error":  
                stop_gunicorn()
                return jsonify({"status": "ERROR", "message": result["message"]}), 500
            elif result.get("status") == "success":
                stop_gunicorn()
                return jsonify(result), 200 
            else:
                stop_gunicorn()
                return jsonify(result), 200
        elif isinstance(result, str):
             stop_gunicorn()
             return Response(result, status=200, mimetype='application/json')
        else:
            stop_gunicorn()
            return jsonify({"status": "SUCCESS", "message": "Processed successfully", "data": result}), 200

    except Exception as e:
        error_message = f"Error in trigger_insights_generation: {e}"
        print(error_message)  
        stop_gunicorn()
        return jsonify({"status": "ERROR", "message": error_message}), 500
    
# Comment out or remove for production with Gunicorn
# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000)