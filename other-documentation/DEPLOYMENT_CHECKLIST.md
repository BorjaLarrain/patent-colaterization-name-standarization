# Quick Deployment Checklist

## ✅ Pre-Deployment Checklist

- [x] `requirements.txt` created with dependencies
- [x] `web_app.py` exists in root directory
- [x] `database_manager.py` exists in root directory
- [x] `database/entities.db` exists and is tracked in git (29MB - this is fine)
- [x] `results/final/*.csv` files exist (for CSV fallback)
- [x] Dependencies verified (streamlit, pandas)

## 🚀 Deployment Steps

1. **Commit new files:**
   ```bash
   git add requirements.txt DEPLOYMENT.md DEPLOYMENT_CHECKLIST.md
   git commit -m "Add deployment files for Streamlit Cloud"
   git push origin main
   ```

2. **Go to Streamlit Community Cloud:**
   - Visit: https://share.streamlit.io
   - Sign in with GitHub

3. **Deploy app:**
   - Click "New app"
   - Repository: `patent-colaterization-name-standarization`
   - Branch: `main`
   - Main file: `web_app.py`
   - App URL: Choose a name (e.g., `entity-review`)

4. **Configure access:**
   - Settings → Visibility → Private
   - Add your professor's email as viewer

5. **Share the link:**
   - Your app URL will be: `https://[your-app-name].streamlit.app`
   - Send this to your professor!

## ⚠️ Important Notes

- Your database file (29MB) is tracked in git - this is fine for GitHub
- The app will work with the database file from your repository
- If you make changes, just push to GitHub and Streamlit will auto-update
- Your repo can stay **private** - Streamlit Cloud can access it after authorization

## 📝 Next Steps After Deployment

After deploying, test the app yourself:
1. Open the Streamlit Cloud URL
2. Click "Load Data" in the sidebar
3. Verify it loads correctly
4. Then share with your professor!


