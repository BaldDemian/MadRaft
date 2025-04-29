#!/bin/bash

success_count=0
fail_count=0

for i in {1..10}
do
    echo "Run #$i: cargo test 3a"
    if cargo test 3a; then
        success_count=$((success_count + 1))
    else
        fail_count=$((fail_count + 1))
    fi

    echo "Run #$i: cargo test 3b"
    if cargo test 3b; then
        success_count=$((success_count + 1))
    else
        fail_count=$((fail_count + 1))
    fi
done

echo "Tests completed:"
echo "Success count: $success_count"
echo "Failure count: $fail_count"

if [ $fail_count -eq 0 ]; then
    echo "All tests passed!"
else
    echo "Some tests failed."
fi
