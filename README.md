# Backblaze B2 Quick Start - Using Python with the Backblaze S3 Compatible API 
This is the code repository for a working sample application for the developer self training series "[Backblaze B2 Quick Start - Using Python with the Backblaze S3 Compatible API](https://backblaze.com/b2/docs/python.html)". It contains all Python code files necessary to implement all of the Backblaze B2 API calls demonstrated in the Quick Start.
## Backblaze B2 Developer Quick Starts
Backblaze B2 Developer Quick Starts provide developers an "instant on" experience for making read-only API calls against Backblaze B2 cloud storage. Plus working source code and simple guided instructions for making create, write and delete API calls against Backblaze B2 cloud storage.   

For learning, Backblaze B2 Developer Quick Starts provide developers the following four integrated resources.  Developers can use any one, or any combination of all four as best fits each developer's style. 

**RESOURCES:**

1. **Sample Application** – Open Source and shared on GitHub for you to download.  Link for [GitHub repository is here](https://github.com/backblaze-b2-samples/b2-python-s3-sample/).

2. **Video Code Walkthroughs of Sample Application** – For you to share and rewatch on demand on YouTube. The [playlist for related videos is here](https://www.youtube.com/c/backblaze/playlists).

3. **Hosted Sample Data** –  A media application with application keys shared for read-only access. The sample media application is hosted in a  Backblaze B2 bucket configured with PUBLIC access.  You can [preview the media application here](https://s3.us-west-002.backblazeb2.com/developer-b2-quick-start/album/photos.html). (Navigate with left/right arrows, or selecting thumbnails along bottom.)

4. **Guided Instructions** – Detailed guided instructions for the full series are [published here](https://backblaze.com/b2/docs/python.html). These instructions guide you how to download the sample code, run it yourself, and then use the code as you see fit. Including incorporating it into your own applications.

If you want to learn the ins-and-outs of Backblaze B2 cloud storage and how to implement solutions with it using Python then this Backblaze B2 Developer Quick Start is for you.  

## Configuration

Copy `.env.example` to `.env` and fill in your Backblaze B2 S3-compatible settings. The sample now expects you to provide your own bucket and credentials.

- `B2_APPLICATION_KEY_ID`
- `B2_APPLICATION_KEY`
- `B2_BUCKET_NAME`
- `B2_REGION`
- `B2_PUBLIC_URL_BASE`

`B2_REGION` is validated before the script derives the S3 endpoint. `B2_PUBLIC_URL_BASE` is the object URL prefix for public links, such as `https://s3.us-west-002.backblazeb2.com/my-bucket` or a custom CDN URL that already includes the bucket path.

For one release, the script also accepts the legacy variable names (`ENDPOINT`, `KEY_ID_RO`, `APPLICATION_KEY_RO`, `KEY_ID_PRIVATE_RO`, `APPLICATION_KEY_PRIVATE_RO`, `ENDPOINT_URL_YOUR_BUCKET`, `KEY_ID_YOUR_ACCOUNT`, and `APPLICATION_KEY_YOUR_ACCOUNT`) as fallbacks. The `B2_*` names take precedence.

### Command buckets and keys

- `01`, `02`, and `02PUB` use `B2_BUCKET_NAME`.
- `02PRI`, `05`, and `06` use `B2_PRIVATE_BUCKET_NAME` and `B2_PRIVATE_APPLICATION_KEY_ID` / `B2_PRIVATE_APPLICATION_KEY` when set. If legacy private-key variables are present, they fall back to the original private demo bucket. Otherwise they use `B2_BUCKET_NAME`.
- `20`, `21`, `30`, `31`, and `32` use `B2_WRITE_BUCKET_NAME` and `B2_WRITE_APPLICATION_KEY_ID` / `B2_WRITE_APPLICATION_KEY` when set. These commands require a read-write application key.
- `22` also requires `B2_TRANSIENT_BUCKET_NAME` for the destination bucket.

The sample sets a custom user agent on every boto3 S3 client. Historical public demo credentials should be treated as exposed; rotate any still-active keys and review B2 access logs before relying on them.
