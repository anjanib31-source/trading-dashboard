## 🔄 Ngrok URL Auto-Update

### How It Works
1. `start_all.bat` starts ngrok
2. Captures the new ngrok URL
3. Saves to `ngrok_url.txt`
4. Pushes to GitHub automatically
5. PWA redirects to the latest URL

### Manual URL Update
```bash
curl http://localhost:4040/api/tunnels
echo "https://your-ngrok-url.ngrok-free.dev" > ngrok_url.txt
git add ngrok_url.txt
git commit -m "🔄 Manual ngrok URL update"
git push origin main