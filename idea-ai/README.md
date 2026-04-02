# IntelliJ IDEA Plugin (Kotlin) Starter

Проектный каркас для разработки плагина под IntelliJ IDEA Ultimate.

## Что уже настроено

- Gradle Kotlin DSL (`build.gradle.kts`, `settings.gradle.kts`)
- Kotlin/JVM toolchain 21
- IntelliJ Platform Gradle Plugin
- `plugin.xml` c action в меню `Tools`
- Стартовый action `HelloAction`

## Структура

- `src/main/kotlin/com/example/ideaai/actions/HelloAction.kt`
- `src/main/resources/META-INF/plugin.xml`
- `build.gradle.kts`
- `settings.gradle.kts`
- `gradle.properties`

## Нужна ли отдельная установка IDEA Ultimate

Не обязательно. В `build.gradle.kts` уже указано:

- `intellijIdeaUltimate("2025.1")`

При запуске `runIde` IntelliJ Platform Gradle Plugin сам скачает нужную версию IDE в Gradle-кэш.

## Подключение JetBrains AI Assistant

1. Открой IntelliJ IDEA Ultimate.
2. `Settings` -> `Plugins` -> `Marketplace`.
3. Найди `AI Assistant` (JetBrains).
4. Нажми `Install` и перезапусти IDE.
5. Войди в JetBrains Account и активируй AI Assistant.

## Первый запуск плагина

1. Открой проект в IntelliJ IDEA (Community/Ultimate - не критично для старта).
2. Дай IDE скачать Gradle зависимости.
3. Запусти задачу `runIde` (она поднимет sandbox IDE на версии из `intellijIdeaUltimate(...)`).
4. Во втором окне IDE открой `Tools` -> `Idea AI: Hello`.
