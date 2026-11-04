#!/usr/bin/env bash
# Cerybro v0.3.0 kurulumu: yedek -> nginx -> kurulum -> kontrol -> hata olursa geri alma
BASE=${CERYBRO_BASE:-/opt/ai-ceo}
SITES=${NGINX_SITES:-/etc/nginx/sites-enabled}
SITES2=${NGINX_CONFD:-/etc/nginx/conf.d}
DOMAIN=cerybro.cairakuzey.cloud
SRC="$(cd "$(dirname "$0")" && pwd)"
TS=$(date +%Y%m%d-%H%M%S)
B=$BASE/backups/v030-$TS
LOG=$B/kurulum.log
mkdir -p "$B" || { echo "HATA: yedek klasoru acilamadi"; exit 1; }
exec > >(tee -a "$LOG") 2>&1

NG=""; NGCHANGED=0

publish_log() {
  echo
  echo "Kurulum kaydi (yalnizca sunucuda): $LOG"
}

restore_nginx() {
  if [ "$NGCHANGED" = "1" ] && [ -f "$B/nginx.conf.old" ]; then
    cp "$B/nginx.conf.old" "$NG" && nginx -t >/dev/null 2>&1 && systemctl reload nginx
    echo "nginx eski haline donduruldu"
    NGCHANGED=0
  fi
}

fail_exit() {
  echo; echo "HATA: $1"
  restore_nginx
  publish_log
  exit 1
}

health_wait() {
  sleep 3
  API=http://$(docker inspect -f "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}" cerybro-api):8000
  for i in $(seq 1 30); do
    if curl -s -m 3 $API/health | grep -q healthy; then return 0; fi
    sleep 2
  done
  return 1
}

echo "== 1/6 Yedek: $B"
cp "$BASE/app/backend/main.py" "$B/main.py.old" || fail_exit "main.py yedeklenemedi"
docker exec cerybro-postgres pg_dump -U cerybro -d cerybro > "$B/db.sql" 2> "$B/db.err"
[ -s "$B/db.sql" ] || fail_exit "veritabani yedegi alinamadi"
echo "yedek tamam (veritabani: $(wc -c < "$B/db.sql") bayt)"

echo "== 2/6 Yeni dosya kontrolu"
[ -f "$SRC/main.py" ] || fail_exit "yeni main.py bulunamadi"
python3 -m py_compile "$SRC/main.py" || fail_exit "yeni main.py yazim hatasi iceriyor"
echo "yeni main.py yazim kontrolu tamam"

echo "== 3/6 Nginx: degisiklik yapilmiyor (api ic agda, 10.0.4.4:8000)"

echo "== 4/6 Yeni surum kuruluyor"
cp "$SRC/main.py" "$BASE/app/backend/main.py" || fail_exit "yeni main.py yerlestirilemedi"
cd "$BASE" || fail_exit "$BASE klasorune girilemedi"
docker compose build api 2>&1 | tail -4
docker compose up -d api 2>&1 | tail -4

echo "== 5/6 Saglik kontrolu"
OK=0
if health_wait \
   && curl -s -m 5 $API/ | grep -q '0.3.0' \
   && curl -s -m 5 $API/ready | grep -q ready \
   && [ "$(curl -s -m 5 -o /dev/null -w '%{http_code}' $API/api/agents)" = "401" ]; then
  OK=1
fi

if [ "$OK" != "1" ]; then
  echo "HATA: yeni surum calismadi. Son kayitlar:"
  docker logs --tail 40 cerybro-api 2>&1
  echo "== Eski surum geri yukleniyor"
  cp "$B/main.py.old" "$BASE/app/backend/main.py"
  docker compose build api 2>&1 | tail -3
  docker compose up -d api 2>&1 | tail -3
  if health_wait; then echo "GERI ALINDI: eski surum calisiyor"; else echo "UYARI: eski surum de acilmadi. Yedekler: $B"; fi
  publish_log
  exit 1
fi

echo "== 6/6 Son durum"
docker ps --format '{{.Names}}: {{.Status}}' | grep -i cerybro
docker logs --tail 8 cerybro-api 2>&1
echo
echo "TAMAM: Cerybro v0.3.0 calisiyor. Yedekler: $B"
publish_log
