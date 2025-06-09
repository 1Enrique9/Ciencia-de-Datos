import asyncio

async def coroutine_a():
    print("Coroutine A: Inicio")
    print("Coroutine A: Esperando a Coroutine B")
    await coroutine_b()
    print("Coroutine A: Reanudada después de Coroutine B")
    print("Coroutine A: Realizando operación I/O (sleep)")
    await asyncio.sleep(1)
    print("Coroutine A: Reanudada después de sleep")
    print("Coroutine A: Fin")

async def coroutine_b():
    print("Coroutine B: Inicio")
    print("Coroutine B: Esperando a Coroutine C")
    await coroutine_c()
    print("Coroutine B: Reanudada después de Coroutine C")
    print("Coroutine B: Fin")

async def coroutine_c():
    print("Coroutine C: Inicio")
    print("Coroutine C: Realizando operación I/O (sleep)")
    await asyncio.sleep(2)
    print("Coroutine C: Reanudada después de sleep")
    print("Coroutine C: Fin")

async def coroutine_d():
    print("Coroutine D: Inicio")
    print("Coroutine D: Realizando operación I/O (sleep)")
    await asyncio.sleep(1)
    print("Coroutine D: Reanudada después de sleep")
    print("Coroutine D: Fin")

async def main():
    print("Main: Creando tareas")
    # Creación de tareas y agregándolas a la cola del event loop
    task1 = asyncio.create_task(coroutine_a())
    task2 = asyncio.create_task(coroutine_d())
    print("Main: Ejecutando tareas")
    # Ejecuta las tareas de forma concurrente
    await asyncio.gather(task1, task2)
    print("Main: Todas las tareas completadas")

if __name__ == "__main__":
    # Inicia el event loop
    asyncio.run(main())
