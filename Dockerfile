FROM python:3.12-slim 

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# On copie tout le répertoire — en Phase 2 on ajoutera des fichiers
# (client HTML, templates, etc.) qui doivent aussi être dans l'image. 
COPY . .

ENV PORT=8080
CMD ["gunicorn", "-k", "gthread", "--workers", "1", "--threads", "8", "--bind", "0.0.0.0:8080", "main:app"]