import json
import time
import threading
import os
import websocket
from flask import Flask, jsonify
from flask_cors import CORS  # Import thư viện CORS

app = Flask(__name__)
CORS(app)  # Kích hoạt CORS cho phép mọi nguồn truy cập

md5_storage = {"htr": []}
MAX_HISTORY = 200
data_lock = threading.Lock()

# URL của bạn (Lưu ý: ConnectionToken có thể hết hạn, nếu tool dừng nhận dữ liệu, cần lấy token mới)
MD5_URL = "wss://taixiu.maksh3979madfw.com/signalr/connect?transport=webSockets&connectionToken=K9JCZcybIrY48JlkOuK31vqaL0TxE4ClQGBGQeG9w9MR8VmI7JM5%2BTNPdy2LlAzu0zdpAtln%2FcFd3TuEz7q%2FYrFn%2Fz8tTQ1kQ2ybR%2BpsgZOM2TAthSnhJwV4IrjaciDc&connectionData=%5B%7B%22name%22%3A%22luckydiceHub%22%7D%5D&tid=3&access_token=05%2F7JlwSPGxS0dh8AXUzJ8LYI6m2%2BGXVB3UwDAmuWFKB5uyRa%2FsLOaYjkY%2B9lcSDLVCmODRrV5Co%2BRJ2PGZ3jH9FF86qYhF%2FL8fj8IAGPMsP45pdIIXZysKmRi40b%2FOVLAp4yOpkaXNIdHRE86OSJC4%2BcoTNWy8oEyMXkO5clUe3lx5JM%2BoUuP%2Fkjgh5xWXtwhgCcTaHR5XqMq%2BFmiGq3xWX7YDHNqICGlY%2FIjZEhMNob4RsFo86yjsko6%2FchcRNc33LSj6kkBnMGb3PvuyxxH5SUGueiC7hw4FPwdAT7XJ76PKkUytQntigqJonYuF870Xqpg3dXXhfZETnnr8OK2a9QYxIa3S%2Fr2J52MwlhrydYpt1ZXVr6bNH8vxo5Qw9kHQfFZtWhmM%3D.7e6dc589c27e16d005041d39bf8b6a45bebbbd90140b2f9548f36c474ca46424"

def parse_md5_message(raw_message):
    try:
        clean_msg = raw_message.replace('\x1e', '')
        if not clean_msg: return
        data = json.loads(clean_msg)
        
        if 'M' in data:
            for message in data['M']:
                if 'A' in message:
                    payload = message['A'][0]
                    # In log ra console của Render để kiểm tra dữ liệu thực tế
                    print(f"DEBUG PAYLOAD: {payload}") 

                    def get_val(p, keys):
                        for k in keys:
                            if isinstance(p, dict) and k in p: return p[k]
                            if isinstance(p, list) and isinstance(k, int) and k < len(p): return p[k]
                        return None

                    res = payload.get('Result') or payload.get('result') or payload
                    
                    new_entry = {
                        "d1": get_val(res, ['Dice1', 'd1', 'v1', 0]),
                        "d2": get_val(res, ['Dice2', 'd2', 'v2', 1]),
                        "d3": get_val(res, ['Dice3', 'd3', 'v3', 2]),
                        "sid": get_val(payload, ['SessionID', 'sid', 'SessionId', 'id'])
                    }

                    d_values = [new_entry["d1"], new_entry["d2"], new_entry["d3"]]
                    if new_entry["sid"] and all(isinstance(v, int) and 1 <= v <= 6 for v in d_values):
                        with data_lock:
                            if not any(item['sid'] == new_entry["sid"] for item in md5_storage["htr"]):
                                md5_storage["htr"].insert(0, new_entry)
                                if len(md5_storage["htr"]) > MAX_HISTORY:
                                    md5_storage["htr"].pop()
                                print(f"Đã cập nhật phiên: {new_entry['sid']}")
    except Exception as e:
        print(f"Lỗi khi xử lý message: {e}")

def start_md5_ws():
    def on_message(ws, message): parse_md5_message(message)
    def on_error(ws, error): print(f"WebSocket Error: {error}")
    def on_close(ws, *args):
        print("WebSocket bị đóng, đang kết nối lại...")
        time.sleep(5)
        threading.Thread(target=start_md5_ws, daemon=True).start()
    
    ws = websocket.WebSocketApp(MD5_URL, on_message=on_message, on_error=on_error, on_close=on_close)
    ws.run_forever()

@app.route('/api/tx')
def get_md5_data():
    with data_lock:
        return jsonify({
            "status": "success", 
            "total": len(md5_storage["htr"]),
            "htr": md5_storage["htr"]
        })

if __name__ == '__main__':
    threading.Thread(target=start_md5_ws, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)
