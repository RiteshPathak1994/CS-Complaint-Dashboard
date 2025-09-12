import dropbox

APP_KEY = "hv4z2pvbjvc3as6"      # replace with your Dropbox app key
APP_SECRET = "2edfewhk9tgvgiq"  # replace with your Dropbox app secret

auth_flow = dropbox.DropboxOAuth2FlowNoRedirect(
    APP_KEY, APP_SECRET, token_access_type="offline"
)

authorize_url = auth_flow.start()
print("1. Go to: " + authorize_url)
print("2. Click 'Allow' (login if needed).")
print("3. Copy the authorization code.")

auth_code = input("Enter the authorization code here: ").strip()
oauth_result = auth_flow.finish(auth_code)

print("\n✅ Save these values in your secrets.toml:")
print(f"DROPBOX_APP_KEY = \"{APP_KEY}\"")
print(f"DROPBOX_APP_SECRET = \"{APP_SECRET}\"")
print(f"DROPBOX_REFRESH_TOKEN = \"{oauth_result.refresh_token}\"")
