{
  "conditions": [
    [
      "OS!=\"win\" and target_arch not in \"ia32 x32 x64\"",
      {
        "defines": [
          "HAVE_DLFCN_H=1"
        ],
        "libraries": [],
        "sources": [
          "deps/cpu_features/include/internal/hwcaps.h",
          "deps/cpu_features/src/hwcaps.c"
        ]
      }
    ]
  ]
}
