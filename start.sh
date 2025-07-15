#!/bin/bash

echo "Kafka 환경 시작"
echo "==============="

# Docker Compose로 Kafka 시작
echo "Kafka + Zookeeper 시작 중..."
docker-compose up -d

# 서비스 준비 대기
echo "서비스 준비 대기 중..."
sleep 10

# 상태 확인
echo ""
echo "서비스 상태:"
docker-compose ps
echo ""
echo "접속 정보:"
echo "  - Kafka: localhost:9092"
echo "  - Redis: localhost:6379"
echo "  - Kafka UI: http://localhost:8080"
echo ""
echo ""
echo "종료 방법:"
echo "  ./stop.sh"
echo "" 