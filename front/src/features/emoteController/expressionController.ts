import * as THREE from 'three'
import {
  VRM,
  VRMExpressionManager,
  VRMExpressionPresetName,
} from '@pixiv/three-vrm'
import { AutoLookAt } from './autoLookAt'
import { AutoBlink } from './autoBlink'

/**
 * 感情ごとの追加ブレンドシェイプ
 * メインの感情プリセットに加えて同時に適用する
 * vrm.update() の後に強制適用するため、overrideMouth 等の影響を受けない
 */
const EMOTION_EXTRA_EXPRESSIONS: Partial<
  Record<
    VRMExpressionPresetName,
    ReadonlyArray<{ preset: VRMExpressionPresetName; value: number }>
  >
> = {
  surprised: [
    { preset: 'aa', value: 0.3 },
  ],
}

/**
 * Expressionを管理するクラス
 *
 * 主に前の表情を保持しておいて次の表情を適用する際に0に戻す作業や、
 * 前の表情が終わるまで待ってから表情適用する役割を持っている。
 */
export class ExpressionController {
  private _autoLookAt: AutoLookAt
  private _autoBlink?: AutoBlink
  private _expressionManager?: VRMExpressionManager
  private _currentEmotion: VRMExpressionPresetName
  private _currentExtraExpressions: ReadonlyArray<{
    preset: VRMExpressionPresetName
    value: number
  }>
  private _currentLipSync: {
    preset: VRMExpressionPresetName
    value: number
  } | null
  constructor(vrm: VRM, camera: THREE.Object3D) {
    this._autoLookAt = new AutoLookAt(vrm, camera)
    this._currentEmotion = 'neutral'
    this._currentExtraExpressions = []
    this._currentLipSync = null
    if (vrm.expressionManager) {
      this._expressionManager = vrm.expressionManager
      this._autoBlink = new AutoBlink(vrm.expressionManager)
    }
  }

  public playEmotion(preset: VRMExpressionPresetName) {
    if (this._currentEmotion != 'neutral') {
      this._expressionManager?.setValue(this._currentEmotion, 0)
    }

    if (preset == 'neutral') {
      this._autoBlink?.setEnable(true)
      this._currentEmotion = preset
      this._currentExtraExpressions = []
      return
    }

    const t = this._autoBlink?.setEnable(false) || 0
    this._currentEmotion = preset
    this._currentExtraExpressions = EMOTION_EXTRA_EXPRESSIONS[preset] ?? []
    setTimeout(() => {
      this._expressionManager?.setValue(preset, 1)
    }, t * 1000)
  }

  public lipSync(preset: VRMExpressionPresetName, value: number) {
    if (this._currentLipSync) {
      this._expressionManager?.setValue(this._currentLipSync.preset, 0)
    }
    this._currentLipSync = {
      preset,
      value,
    }
  }

  public update(delta: number) {
    if (this._autoBlink) {
      this._autoBlink.update(delta)
    }

    if (this._currentLipSync) {
      const weight =
        this._currentEmotion === 'neutral'
          ? this._currentLipSync.value * 0.5
          : this._currentLipSync.value * 0.25
      this._expressionManager?.setValue(this._currentLipSync.preset, weight)
    }
  }

  /**
   * vrm.update() の後に呼び出し、追加ブレンドシェイプを強制適用する
   * VRM の override 処理（overrideMouth 等）を回避するため、
   * expression.applyWeight() で直接モーフターゲットを上書きする
   */
  public applyExtraExpressions(): void {
    if (this._currentExtraExpressions.length === 0) return
    if (!this._expressionManager) return

    for (const extra of this._currentExtraExpressions) {
      const expression = this._expressionManager.getExpression(extra.preset)
      if (!expression) continue
      expression.weight = extra.value
      expression.applyWeight()
    }
  }
}
