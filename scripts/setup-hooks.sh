#!/bin/bash

cat > .git/hooks/pre-push << 'HOOK'
#!/bin/bash
docker build -t comp585-recommender:m2 . -q
docker run --rm comp585-recommender:m2 pytest tests/ -v --tb=short

if [ $? -eq 0 ]; then
        echo "Tests passed - push modifications"
        exit 0
else
        echo "Tests failed - push blocked"
        exit 1
fi
HOOK

chmod +x .git/hooks/pre-push


