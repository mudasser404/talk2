import runpod
import os
import time
import json
import uuid
import base64
import requests
import websocket
import logging
import librosa

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

COMFY_URL = "http://127.0.0.1:8188"
WS_URL = "ws://127.0.0.1:8188/ws"

def load_workflow(path):
    with open(path, "r") as f:
        return json.load(f)

def wait_for_comfy():
    for i in range(180):
        try:
            r = requests.get(COMFY_URL)
            if r.status_code == 200:
                logger.info("ComfyUI is ready.")
                return
        except Exception:
            pass
        logger.info("Waiting for ComfyUI...")
        time.sleep(1)
    raise RuntimeError("ComfyUI did not start.")

def get_audio_duration(audio_path):
    try:
        duration = librosa.get_duration(path=audio_path)
        return duration
    except Exception as e:
        logger.warning(f"Failed to calculate audio duration ({audio_path}): {e}")
        return None

def queue_prompt(prompt):
    r = requests.post(
        f"{COMFY_URL}/prompt",
        json={"prompt": prompt}
    )
    r.raise_for_status()
    return r.json()["prompt_id"]

def wait_for_prompt(prompt_id):
    ws = websocket.WebSocket()
    ws.connect(WS_URL)
    try:
        while True:
            msg = ws.recv()
            if msg:
                data = json.loads(msg)
                if data.get("type") == "executed" and data["data"].get("prompt_id") == prompt_id:
                    return
    finally:
        ws.close()

def handler(event):
    wait_for_comfy()

    input_data = event["input"]

    workflow_name = input_data.get("workflow", "singlespeaker")
    workflow_path = f"/workflows/{workflow_name}.json"

    prompt = load_workflow(workflow_path)

    job_id = str(uuid.uuid4())

    output_dir = f"/tmp/{job_id}"
    os.makedirs(output_dir, exist_ok=True)

    audio_input = input_data["audio"]
    image_input = input_data["image"]

    audio_path = os.path.join(output_dir, "audio.wav")
    image_path = os.path.join(output_dir, "image.png")

    if audio_input.startswith("http"):
        r = requests.get(audio_input)
        with open(audio_path, "wb") as f:
            f.write(r.content)
    else:
        with open(audio_path, "wb") as f:
            f.write(base64.b64decode(audio_input))

    if image_input.startswith("http"):
        r = requests.get(image_input)
        with open(image_path, "wb") as f:
            f.write(r.content)
    else:
        with open(image_path, "wb") as f:
            f.write(base64.b64decode(image_input))

    audio_duration = get_audio_duration(audio_path)
    if audio_duration is None:
        raise RuntimeError("Could not calculate audio duration.")

    # Apply inputs to the workflow
    for node in prompt.values():
        if node.get("class_type") == "LoadImage":
            node["inputs"]["image"] = image_path
        if node.get("class_type") == "LoadAudio":
            node["inputs"]["audio"] = audio_path
        if node.get("class_type") == "AudioDuration":
            node["inputs"]["duration"] = audio_duration

    prompt_id = queue_prompt(prompt)
    wait_for_prompt(prompt_id)

    # Output file path
    output_video = os.path.join(output_dir, "output.mp4")

    if input_data.get("network_volume"):
        final_path = f"/runpod-volume/{job_id}.mp4"
        os.rename(output_video, final_path)
        return {"video_path": final_path}
    else:
        with open(output_video, "rb") as f:
            encoded = base64.b64encode(f.read()).decode("utf-8")
        return {"video_base64": encoded}

runpod.serverless.start({"handler": handler})
