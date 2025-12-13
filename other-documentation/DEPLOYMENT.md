# Streamlit Community Cloud Deployment Guide

This guide will help you deploy the Entity Review web app to Streamlit Community Cloud so your professor can access it without cloning the repository.

## Prerequisites

1. **GitHub account** (free)
2. **Streamlit Community Cloud account** (free) - sign up at https://share.streamlit.io
3. Your code pushed to a GitHub repository (can be private)

## Step-by-Step Deployment

### Step 1: Ensure Your Repository is Ready

Make sure your repository contains:
- ✅ `web_app.py` (main Streamlit app)
- ✅ `database_manager.py` (database module)
- ✅ `requirements.txt` (dependencies - already created)
- ✅ `database/entities.db` (database file - needed for the app to work)
- ✅ `results/final/*.csv` (mapping files - needed for initial data)

### Step 2: Push to GitHub

If you haven't already, push your code to GitHub:

```bash
git add .
git commit -m "Add requirements.txt and prepare for deployment"
git push origin main
```

**Note:** Your repository can be **private**. Streamlit Cloud can access private repos after you authorize it.

### Step 3: Sign Up / Log In to Streamlit Community Cloud

1. Go to https://share.streamlit.io
2. Click **"Sign up"** or **"Log in"**
3. Sign in with your **GitHub account** (this authorizes Streamlit to access your repos)

### Step 4: Deploy Your App

1. Click **"New app"** button
2. Fill in the deployment form:
   - **Repository:** Select your repository (`patent-colaterization-name-standarization`)
   - **Branch:** `main` (or your default branch)
   - **Main file path:** `web_app.py`
   - **App URL:** Choose a custom URL (e.g., `entity-review-app`)
3. Click **"Deploy"**

### Step 5: Wait for Deployment

- Streamlit will install dependencies from `requirements.txt`
- It will run your app
- This usually takes 1-3 minutes
- You'll see build logs in real-time

### Step 6: Configure App Access

Once deployed, you can control who can access your app:

1. Go to your app's settings (click the **"⋮"** menu → **"Settings"**)
2. Under **"Visibility"**, choose:
   - **"Public"** - Anyone with the link can access
   - **"Private"** - Only you and people you add can access
3. If **Private**, click **"Add viewer"** and enter your professor's email address

### Step 7: Share the Link

Your app will have a URL like:
```
https://entity-review-app.streamlit.app
```

Send this link to your professor! They can access it directly in their browser.

---

## Troubleshooting

### App fails to deploy

**Check:**
- ✅ `requirements.txt` exists and has correct dependencies
- ✅ `web_app.py` is in the root directory
- ✅ Database file (`database/entities.db`) exists in the repo
- ✅ CSV files in `results/final/` exist

**Common issues:**
- **Missing database file:** The app needs `database/entities.db` to work. Make sure it's committed to Git (it might be in `.gitignore` - you may need to remove it from there temporarily).
- **Import errors:** Check that all Python files are in the correct locations relative to `web_app.py`

### Database file is too large for Git

If `entities.db` is too large (>100MB), you have options:

1. **Use CSV fallback:** The app can import from CSV if database doesn't exist
2. **Initialize empty database:** Create a minimal database that gets populated on first run
3. **Use Git LFS:** For very large files, use Git Large File Storage

### App loads but shows errors

- Check the app logs in Streamlit Cloud dashboard
- Make sure all data files (`database/entities.db`, CSV files) are in the repository
- Verify file paths are correct (they should be relative to the repo root)

---

## Updating Your App

After making changes:

1. Push changes to GitHub:
   ```bash
   git add .
   git commit -m "Update app"
   git push origin main
   ```

2. Streamlit Cloud will **automatically redeploy** your app (usually within 1-2 minutes)

3. Your professor will see the updated version automatically!

---

## Security Notes

- ✅ Your **GitHub repository can stay private** - Streamlit Cloud can access it after authorization
- ✅ Your **app can be private** - only share the link with your professor
- ✅ Database and data files are stored securely on Streamlit's servers
- ⚠️ **Don't commit sensitive data** (API keys, passwords, etc.) to your repository

---

## Need Help?

- Streamlit Community Cloud docs: https://docs.streamlit.io/deploy/streamlit-community-cloud
- Streamlit Community forum: https://discuss.streamlit.io


