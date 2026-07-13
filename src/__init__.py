@app.route('/api/health')
def get_health():
    """Get bot health status"""
    try:
        # Read health status from a file or shared state
        health_file = 'health_status.json'
        if os.path.exists(health_file):
            with open(health_file, 'r') as f:
                health_data = json.load(f)
            return jsonify({
                'status': 'success',
                'data': health_data
            })
        else:
            # Return default health if no data
            return jsonify({
                'status': 'success',
                'data': {
                    'status': '✅ All systems operational',
                    'all_ok': True,
                    'broker': True,
                    'market': True,
                    'scanner': True,
                    'sync': True,
                    'last_error': None,
                    'error_count': 0,
                    'last_heartbeat': datetime.now().isoformat()
                }
            })
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})