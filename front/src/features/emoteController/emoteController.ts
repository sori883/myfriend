import { VRM, VRMExpressionPresetName } from '@pixiv/three-vrm'
import * as THREE from 'three'
import { ExpressionController } from './expressionController'
import { MotionController } from './motionController'
import { EMOTIONS, EmotionType } from '@/features/messages/messages'

const EMOTION_SET: ReadonlySet<string> = new Set(EMOTIONS)

/**
 * 感情表現としてExpressionとMotionを操作する為のクラス
 */
export class EmoteController {
  private _expressionController: ExpressionController
  private _motionController: MotionController

  constructor(vrm: VRM, camera: THREE.Object3D) {
    this._expressionController = new ExpressionController(vrm, camera)
    this._motionController = new MotionController()
  }

  public playEmotion(preset: VRMExpressionPresetName) {
    this._expressionController.playEmotion(preset)
    if (EMOTION_SET.has(preset)) {
      this._motionController.setEmotion(preset as EmotionType)
    }
  }

  public lipSync(preset: VRMExpressionPresetName, value: number) {
    this._expressionController.lipSync(preset, value)
  }

  public update(delta: number) {
    this._expressionController.update(delta)
    this._motionController.update(delta)
  }

  /**
   * ボディジェスチャーをボーンに加算適用する
   * mixer.update() の後、vrm.update() の前に呼ぶこと
   */
  public applyGesture(vrm: VRM) {
    this._motionController.applyGesture(vrm)
  }

  /**
   * vrm.update() の後に呼び出し、追加ブレンドシェイプを強制適用する
   */
  public applyExtraExpressions() {
    this._expressionController.applyExtraExpressions()
  }
}
