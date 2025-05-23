#!/usr/bin/env python3
import os
import signal
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
from initializer import insights_generator
from metadata_generation import metadata_generator
from ask_summary_generation import ask_summary_generator

app = Flask(__name__)
CORS(app)

def stop_gunicorn():
    """
    Stops the Gunicorn master process.  This should be called after 
    application has finished processing its task and sent the response.
    """
    server_pid = os.getppid()  # Get the parent process ID (Gunicorn master)
    os.kill(server_pid, signal.SIGTERM)  # Send a termination signal


@app.route("/metadata", methods=["POST"])
def metadata_generation():
    try:
        event = request.get_json()
        result = metadata_generator(event)

        if isinstance(result, dict):
            # stop_gunicorn()
            return jsonify(result), 200
        elif isinstance(result, str):
            # stop_gunicorn()
            return Response(result, status=200, mimetype='application/json')
    except Exception as e:
        error_message = f"Error in metadata_generation: {e}"
        # stop_gunicorn()
        return jsonify({"status": "ERROR", "message": error_message}), 500



@app.route("/trigger", methods=["POST"])
def trigger_insights_generation():
    try:
        event = request.get_json()
        print(f"Received /trigger request from {request.remote_addr} with payload: {event}")
        result = insights_generator(event)

        # return "Hello World"
        if isinstance(result, dict): 
            print(result.get("status"))
            if result.get("status") == "error":  
                # stop_gunicorn()
                return jsonify({"status": "ERROR", "message": result["message"]}), 500
            elif result.get("status") == "success":
                # stop_gunicorn()
                return jsonify(result), 200 
            else:
                # stop_gunicorn()
                return jsonify(result), 200
        elif isinstance(result, str):
            #  stop_gunicorn()
             return Response(result, status=200, mimetype='application/json')
        else:
            # stop_gunicorn()
            return jsonify({"status": "SUCCESS", "message": "Processed successfully", "data": result}), 200

    except Exception as e:
        error_message = f"Error in trigger_insights_generation: {e}"
        print(error_message)  
        # stop_gunicorn()
        return jsonify({"status": "ERROR", "message": error_message}), 500
    

@app.route("/ask", methods=["POST"])
def ask_summary_generation():
    try:
        event = request.get_json()
        result = ask_summary_generator(event)

        if isinstance(result, dict):
            # stop_gunicorn()
            return jsonify(result), 200
        elif isinstance(result, str):
            # stop_gunicorn()
            return Response(result, status=200, mimetype='application/json')
    except Exception as e:
        error_message = f"Error in ask_summary_generation: {e}"
        # stop_gunicorn()
        return jsonify({"status": "ERROR", "message": error_message}), 500
