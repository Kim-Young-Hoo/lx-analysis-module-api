def coroutine_example():
    while True:
        received_value = yield  # Pauses the coroutine and returns a value to the caller
        print('Received:', received_value)

# Create a coroutine object
coroutine = coroutine_example()

# Advance the coroutine to the first `yield` statement (initially, it's not actually yielding anything)
next(coroutine)

# Send a value into the coroutine
coroutine.send('Hello')
coroutine.send('World')

