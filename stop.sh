#!/bin/bash

echo "Kafka 환경 정리"
echo "==============="

# Docker Compose 정리
echo "Docker Compose 서비스 정리 중..."
docker-compose down

# 볼륨도 삭제할지 선택
echo ""
read -p "데이터 볼륨도 삭제하시겠습니까? (y/N): " delete_volumes

if [[ "$delete_volumes" =~ ^[Yy]$ ]]; then
    echo "볼륨 삭제 중..."
    docker-compose down -v
    docker volume prune -f
    echo "볼륨이 삭제되었습니다."
else
    echo "볼륨은 보존됩니다."
fi

echo ""
echo "정리 완료" 